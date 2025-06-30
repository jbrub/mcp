# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""awslabs kinesis MCP Server implementation."""

# All imports consolidated at the top
import boto3
import os
import re
from awslabs.kinesis_mcp_server.common import (
    AddTagsToStreamInput,
    CreateStreamInput,
    DescribeStreamConsumerInput,
    DescribeStreamInput,
    DescribeStreamSummaryInput,
    EnableEnhancedMonitoringInput,
    GetRecordsInput,
    GetResourcePolicyInput,
    GetShardIteratorInput,
    IncreaseStreamRetentionPeriodInput,
    ListShardsInput,
    ListStreamConsumersInput,
    ListStreamsInput,
    ListTagsForResourceInput,
    ListTagsForStreamInput,
    PutRecordsInput,
    PutResourcePolicyInput,
    TagResourceInput,
    handle_exceptions,
)
from awslabs.kinesis_mcp_server.consts import (
    DEFAULT_GET_RECORDS_LIMIT,
    DEFAULT_MAX_RESULTS,
    # Defaults
    DEFAULT_REGION,
    DEFAULT_SHARD_COUNT,
    DEFAULT_STREAM_LIMIT,
    DEFUALT_MAX_RESULTS,
    MAX_LENGTH_SHARD_ITERATOR,
    MAX_LIMIT,
    MAX_RECORDS,
    MAX_RESULTS_PER_STREAM,
    MAX_SHARD_ID_LENGTH,
    MAX_SHARD_LEVEL_METRICS,
    MAX_SHARDS_PER_STREAM,
    MAX_STREAM_ARN_LENGTH,
    # Shared constants
    MAX_STREAM_NAME_LENGTH,
    MAX_TAG_KEY_LENGTH,
    MAX_TAG_VALUE_LENGTH,
    MAX_TAGS_COUNT,
    MIN_LENGTH_SHARD_ITERATOR,
    MIN_LIMIT,
    MIN_RECORDS,
    MIN_RESULTS_PER_STREAM,
    MIN_SHARD_ID_LENGTH,
    MIN_SHARD_LEVEL_METRICS,
    MIN_SHARDS_PER_STREAM,
    MIN_STREAM_ARN_LENGTH,
    MIN_STREAM_NAME_LENGTH,
    MIN_TAG_KEY_LENGTH,
    STREAM_MODE_ON_DEMAND,
    STREAM_MODE_PROVISIONED,
    VALID_SHARD_ITERATOR_TYPES,
    VALID_SHARD_LEVEL_METRICS,
)
from botocore.config import Config
from datetime import datetime
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List, Union


# MCP Server Declaration
mcp = FastMCP(
    'awslabs.kinesis-mcp-server',
    instructions="""
    This Kinesis MCP server provides tools to interact with Amazon Kinesis Data Streams.

    When using these tools, please specify all relevant parameters explicitly, even when using default values.
    For example, when creating a stream, include the region_name parameter even if using the default region.

    The default region being used is 'us-west-2'. A region must be explicitly stated to use any other region.

    This helps ensure clarity and prevents region-related issues when working with AWS resources.
    """,
    version='1.0',
)


@handle_exceptions
def get_kinesis_client(region_name: str = DEFAULT_REGION):
    """Create a boto3 Kinesis client using credentials from environment variables. Falls back to 'us-west-2' if no region is specified or found in environment."""
    # Use provided region, or get from env, or fall back to us-west-2
    region = region_name or os.getenv('AWS_REGION') or 'us-west-2'

    # Configure custom user agent to identify requests from LLM/MCP
    config = Config(user_agent_extra='MCP/KinesisServer')

    # Create a new session to force credentials to reload
    # so that if user changes credential, it will be reflected immediately in the next call
    session = boto3.Session()

    # boto3 will automatically load credentials from environment variables:
    # AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
    return session.client('kinesis', region_name=region, config=config)


@mcp.tool('put_records')
@handle_exceptions
def put_records(
    records: List[Dict[str, Any]],
    stream_name: str = None,
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Writes multiple data records to a Kinesis data stream in a single call.

    Args:
        records: List of records to write to the stream
        stream_name: The name of the stream to write to
        stream_arn: ARN of the stream to write to
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the sequence number and shard ID of the records
    """
    # Validate records
    if not records:
        raise ValueError('records is required')

    if not isinstance(records, list):
        raise TypeError('records must be a list')

    if len(records) < MIN_RECORDS or len(records) > MAX_RECORDS:
        raise ValueError(f'Number of records must be between {MIN_RECORDS} and {MAX_RECORDS}')

    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Build parameters
    params: PutRecordsInput = {'Records': records}

    # Add optional parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to put records
    kinesis = get_kinesis_client(region_name)
    response = kinesis.put_records(**params)

    # Return Sequence Number and Shard ID
    return response


@mcp.tool('get_records')
@handle_exceptions
def get_records(
    shard_iterator: str,
    limit: int = DEFAULT_GET_RECORDS_LIMIT,
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Retrieves records from a Kinesis shard.

    Args:
        shard_iterator: The shard iterator to use for retrieving records
        limit: Maximum number of records to retrieve (default: None)
        stream_arn: ARN of the stream to retrieve records from
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the retrieved records
    """
    # Validate shard_iterator
    if not shard_iterator:
        raise ValueError('shard_iterator is required')

    if (
        len(shard_iterator) < MIN_LENGTH_SHARD_ITERATOR
        or len(shard_iterator) > MAX_LENGTH_SHARD_ITERATOR
    ):
        raise ValueError(
            f'shard_iterator length must be between {MIN_LENGTH_SHARD_ITERATOR} and {MAX_LENGTH_SHARD_ITERATOR} characters'
        )

    # Validate limit
    if limit is not None:
        if not isinstance(limit, int):
            raise TypeError('limit must be an integer')

        if limit < MIN_LIMIT or limit > MAX_LIMIT:
            raise ValueError(f'limit must be between {MIN_LIMIT} and {MAX_LIMIT}')

    # Validate stream_arn if provided
    if stream_arn is not None:
        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Build parameters
    params: GetRecordsInput = {'ShardIterator': shard_iterator}

    # Add optional parameters
    if limit is not None:
        params['Limit'] = limit

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to get records
    kinesis = get_kinesis_client(region_name)
    response = kinesis.get_records(**params)

    # Return Records
    return response


@mcp.tool('create_stream')
@handle_exceptions
def create_stream(
    stream_name: str,
    shard_count: int = None,
    stream_mode_details: Dict[str, str] = {'StreamMode': STREAM_MODE_ON_DEMAND},
    tags: Dict[str, str] = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Creates a new Kinesis data stream with the specified name and shard count.

    Args:
        stream_name: A name to identify the stream, must follow Kinesis naming conventions
        shard_count: Number of shards to create (default: 1), only used if stream_mode_details is set to PROVISIONED
        stream_mode_details: Details about the stream mode (default: {"StreamMode": "ON_DEMAND"})
        tags: Tags to associate with the stream
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the stream name and creation status
    """
    # Validate stream_name
    if not stream_name:
        raise ValueError('stream_name is required')

    if not isinstance(stream_name, str):
        raise TypeError('stream_name must be a string')

    if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
        raise ValueError(
            f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
        )

    if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
        raise ValueError(
            'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
        )

    if stream_name.lower().startswith('aws:'):
        raise ValueError('stream_name cannot start with "aws:"')

    # Validate shard_count if provided
    if shard_count is not None:
        if not isinstance(shard_count, int):
            raise TypeError('shard_count must be an integer')

        if shard_count < MIN_SHARDS_PER_STREAM or shard_count > MAX_SHARDS_PER_STREAM:
            raise ValueError(f'shard_count must be between 1 and {MAX_SHARDS_PER_STREAM}')

    # Validate stream_mode_details
    if stream_mode_details is not None and not isinstance(stream_mode_details, dict):
        raise TypeError('stream_mode_details must be a dictionary')

    # Validate tags
    if tags is not None:
        if not isinstance(tags, dict):
            raise TypeError('tags must be a dictionary')

        if len(tags) > MAX_TAGS_COUNT:
            raise ValueError(f'Number of tags cannot exceed {MAX_TAGS_COUNT}')

        # Validate tag keys and values
        for key, value in tags.items():
            if not isinstance(key, str) or not isinstance(value, str):
                raise TypeError('Tag keys and values must be strings')

            if len(key) < MIN_TAG_KEY_LENGTH or len(key) > MAX_TAG_KEY_LENGTH:
                raise ValueError(
                    f'Tag key length must be between {MIN_TAG_KEY_LENGTH} and {MAX_TAG_KEY_LENGTH} characters'
                )

            if len(value) > MAX_TAG_VALUE_LENGTH:
                raise ValueError(
                    f'Tag value length cannot exceed {MAX_TAG_VALUE_LENGTH} characters'
                )

    # Build parameters
    params: CreateStreamInput = {'StreamName': stream_name}

    params['StreamModeDetails'] = stream_mode_details

    # Add ShardCount only for PROVISIONED mode
    if stream_mode_details.get('StreamMode') == STREAM_MODE_PROVISIONED:
        params['ShardCount'] = DEFAULT_SHARD_COUNT if shard_count is None else shard_count

    # Add tags if provided
    if tags is not None:
        params['Tags'] = tags

    # Call Kinesis API to create the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.create_stream(**params)

    return response


@mcp.tool('list_streams')
@handle_exceptions
def list_streams(
    exclusive_start_stream_name: str = None,
    limit: int = DEFAULT_STREAM_LIMIT,
    next_token: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the Kinesis data streams.

    Args:
        exclusive_start_stream_name: Name of the stream to start listing from (default: None)
        limit: Maximum number of streams to list (default: 100)
        next_token: Token for pagination (default: None)
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing stream names, hasMoreStreams flag, and nextToken for pagination
    """
    # Initialize parameters
    params: ListStreamsInput = {}

    # Validate exclusive_start_stream_name if provided
    if exclusive_start_stream_name is not None:
        if not isinstance(exclusive_start_stream_name, str):
            raise TypeError('exclusive_start_stream_name must be a string')

        if (
            len(exclusive_start_stream_name) < MIN_STREAM_NAME_LENGTH
            or len(exclusive_start_stream_name) > MAX_STREAM_NAME_LENGTH
        ):
            raise ValueError(
                f'exclusive_start_stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

        params['ExclusiveStartStreamName'] = exclusive_start_stream_name

    # Validate limit if provided
    if limit is not None:
        if not isinstance(limit, int):
            raise TypeError('limit must be an integer')

        if limit < 1 or limit > DEFAULT_STREAM_LIMIT:
            raise ValueError(f'limit must be between 1 and {DEFAULT_STREAM_LIMIT}')

        params['Limit'] = limit

    # Validate next_token if provided
    if next_token is not None:
        if not isinstance(next_token, str):
            raise TypeError('next_token must be a string')

        params['NextToken'] = next_token

    # Call Kinesis API to list the streams
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_streams(**params)

    return response


@mcp.tool('describe_stream_summary')
@handle_exceptions
def describe_stream_summary(
    stream_name: str = None,
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes the stream summary.

    Args:
        stream_name: Name of the stream to describe
        stream_arn: ARN of the stream to describe
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing stream summary details
    """
    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Initialize parameters
    params: DescribeStreamSummaryInput = {}

    # Call Kinesis API to describe the stream summary
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_stream_summary(**params)

    # Return Stream Summary Details
    return response


@mcp.tool('get_shard_iterator')
@handle_exceptions
def get_shard_iterator(
    shard_id: str,
    shard_iterator_type: str,
    stream_name: str = None,
    stream_arn: str = None,
    starting_sequence_number: str = None,
    timestamp: Union[datetime, str] = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Retrieves a shard iterator for a specified shard.

    Args:
        shard_id: Shard ID to retrieve the iterator for
        shard_iterator_type: Type of shard iterator to retrieve (AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST, AT_TIMESTAMP)
        stream_name: Name of the stream to retrieve the shard iterator for
        stream_arn: ARN of the stream to retrieve the shard iterator for
        starting_sequence_number: Sequence number to start retrieving records from
        timestamp: Timestamp to start retrieving records from
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the shard iterator
    """
    # Validate shard_id
    if not shard_id:
        raise ValueError('shard_id is required')
    if not isinstance(shard_id, str):
        raise TypeError('shard_id must be a string')
    if len(shard_id) < MIN_SHARD_ID_LENGTH or len(shard_id) > MAX_SHARD_ID_LENGTH:
        raise ValueError(
            f'shard_id length must be between {MIN_SHARD_ID_LENGTH} and {MAX_SHARD_ID_LENGTH} characters'
        )
    if not re.match(r'^[a-zA-Z0-9_.-]+$', shard_id):
        raise ValueError(
            'shard_id can only contain alphanumeric characters, underscores, periods, and hyphens'
        )

    # Validate shard_iterator_type
    if shard_iterator_type not in VALID_SHARD_ITERATOR_TYPES:
        raise ValueError(f'shard_iterator_type must be one of {VALID_SHARD_ITERATOR_TYPES}')

    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Validate starting_sequence_number if required
    if (
        shard_iterator_type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']
        and starting_sequence_number is None
    ):
        raise ValueError(
            'starting_sequence_number is required for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER shard iterator types'
        )

    if starting_sequence_number is not None and not isinstance(starting_sequence_number, str):
        raise TypeError('starting_sequence_number must be a string')

    # Validate timestamp if required
    if shard_iterator_type == 'AT_TIMESTAMP' and timestamp is None:
        raise ValueError('timestamp is required for AT_TIMESTAMP shard iterator type')
    if timestamp is not None and not isinstance(timestamp, (datetime, str)):
        raise TypeError('timestamp must be a datetime object or string')

    # Build Paramaters
    params: GetShardIteratorInput = {
        'ShardId': shard_id,
        'ShardIteratorType': shard_iterator_type,
    }
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    if starting_sequence_number is not None:
        params['StartingSequenceNumber'] = starting_sequence_number

    if timestamp is not None:
        params['Timestamp'] = timestamp

    # Call Kinesis API to get the shard iterator
    kinesis = get_kinesis_client(region_name)
    response = kinesis.get_shard_iterator(**params)

    # Return Shard Iterator Details
    return response


# =========================================================
#                   Stretch Goal APIs
# =========================================================


@mcp.tool('add_tags_to_stream')
@handle_exceptions
def add_tags_to_stream(
    tags: Dict[str, str],
    stream_name: str = None,
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Adds tags to a Kinesis data stream.

    Args:
        stream_name: Name of the stream to add tags to
        stream_arn: ARN of the stream to add tags to
        tags: Tags to associate with the stream
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the response from the Kinesis API
    """
    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate tags
    if not tags:
        raise ValueError('tags is required')

    if not isinstance(tags, dict):
        raise TypeError('tags must be a dictionary')

    if len(tags) > MAX_TAGS_COUNT:
        raise ValueError(f'Number of tags cannot exceed {MAX_TAGS_COUNT}')

    # Validate tag keys and values
    for key, value in tags.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise TypeError('Tag keys and values must be strings')

        if len(key) < MIN_TAG_KEY_LENGTH or len(key) > MAX_TAG_KEY_LENGTH:
            raise ValueError(
                f'Tag key length must be between {MIN_TAG_KEY_LENGTH} and {MAX_TAG_KEY_LENGTH} characters'
            )

        if len(value) > MAX_TAG_VALUE_LENGTH:
            raise ValueError(f'Tag value length cannot exceed {MAX_TAG_VALUE_LENGTH} characters')

    # Build parameters
    params: AddTagsToStreamInput = {'Tags': tags}

    # Add StreamName or StreamARN
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to add tags to the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.add_tags_to_stream(**params)

    return response


@mcp.tool('describe_stream')  # TODO: Works - need to add testing
@handle_exceptions
def describe_stream(
    stream_name: str = None,
    stream_arn: str = None,
    limit: int = DEFAULT_STREAM_LIMIT,
    exclusive_start_shard_id: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes the stream.

    Args:
        stream_name: Name of the stream to describe
        stream_arn: ARN of the stream to describe
        limit: Maximum number of shards to return
        exclusive_start_shard_id: Shard ID to start listing from
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing stream details
    """
    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Validate limit if provided
    if limit is not None:
        if not isinstance(limit, int):
            raise TypeError('limit must be an integer')

        if limit < MIN_LIMIT or limit > MAX_LIMIT:
            raise ValueError(f'limit must be between {MIN_LIMIT} and {MAX_LIMIT}')

    # Validate exclusive_start_shard_id if provided
    if exclusive_start_shard_id is not None:
        if not isinstance(exclusive_start_shard_id, str):
            raise TypeError('exclusive_start_shard_id must be a string')

        if not re.match(r'^[a-zA-Z0-9_.-]+$', exclusive_start_shard_id):
            raise ValueError(
                'exclusive_start_shard_id can only contain alphanumeric characters, underscores, periods, and hyphens'
            )
        if (
            len(exclusive_start_shard_id) < MIN_SHARD_ID_LENGTH
            or len(exclusive_start_shard_id) > MAX_SHARD_ID_LENGTH
        ):
            raise ValueError(
                f'exclusive_start_shard_id length must be between {MIN_SHARD_ID_LENGTH} and {MAX_SHARD_ID_LENGTH} characters'
            )

    # Initialize parameters
    params: DescribeStreamInput = {}

    if stream_name is not None:
        params['StreamName'] = stream_name
    if stream_arn is not None:
        params['StreamARN'] = stream_arn
    if limit is not None:
        params['Limit'] = limit
    if exclusive_start_shard_id is not None:
        params['ExclusiveStartShardId'] = exclusive_start_shard_id

    # Call Kinesis API to describe the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_stream(**params)

    # Return Stream Details
    return response


@mcp.tool('describe_stream_consumer')
@handle_exceptions
def describe_stream_consumer(
    consumer_name: str = None,
    stream_arn: str = None,
    consumer_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes a Kinesis data stream consumer.

    Args:
        consumer_name: Name of the consumer to describe
        stream_arn: ARN of the stream the consumer is associated with
        consumer_arn: ARN of the consumer to describe
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the consumer details
    """
    # Validate consumer identification
    if not consumer_name and not stream_arn:
        raise ValueError('Either consumer_name or stream_arn must be provided')

    # Validate consumer_name
    if not consumer_name:
        raise ValueError('consumer_name is required')

    if not isinstance(consumer_name, str):
        raise TypeError('consumer_name must be a string')

    # Validate stream_arn
    if not stream_arn:
        raise ValueError('stream_arn is required')

    if not isinstance(stream_arn, str):
        raise TypeError('stream_arn must be a string')

    if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
        raise ValueError(
            f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
        )

    # Build parameters
    params: DescribeStreamConsumerInput = {}

    if consumer_name is not None:
        params['ConsumerName'] = consumer_name
    if stream_arn is not None:
        params['StreamARN'] = stream_arn
    if consumer_arn is not None:
        params['ConsumerARN'] = consumer_arn

    # Call Kinesis API to describe the stream consumer
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_stream_consumer(**params)

    return response


@mcp.tool('list_stream_consumers')
@handle_exceptions
def list_stream_consumers(
    stream_arn: str,
    next_token: str = None,
    stream_creation_time_stamp: Union[datetime, str] = None,
    max_results: int = DEFUALT_MAX_RESULTS,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the consumers of a Kinesis data stream.

    Args:
        stream_arn: ARN of the stream to list consumers for
        next_token: Token for pagination (default: None)
        stream_creation_time_stamp: Timestamp to filter consumers created after this time
        max_results: Maximum number of results to return (default: 100)
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the list of consumers and pagination details
    """
    # Validate stream_arn
    if not stream_arn:
        raise ValueError('stream_arn is required')

    if not isinstance(stream_arn, str):
        raise TypeError('stream_arn must be a string')

    if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
        raise ValueError(
            f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
        )

    # Validate next_token if provided
    if next_token is not None:
        if not isinstance(next_token, str):
            raise TypeError('next_token must be a string')

    # Validate max_results
    if max_results is not None:
        if not isinstance(max_results, int):
            raise TypeError('max_results must be an integer')

        if max_results < MIN_RESULTS_PER_STREAM or max_results > MAX_RESULTS_PER_STREAM:
            raise ValueError(
                f'max_results must be between {MIN_RESULTS_PER_STREAM} and {MAX_RESULTS_PER_STREAM}'
            )

    # Build parameters
    params: ListStreamConsumersInput = {'StreamARN': stream_arn}

    if next_token is not None:
        params['NextToken'] = next_token

    if stream_creation_time_stamp is not None:
        params['StreamCreationTimestamp'] = stream_creation_time_stamp

    if max_results is not None:
        params['MaxResults'] = max_results

    # Call Kinesis API to list the stream consumers
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_stream_consumers(**params)

    return response


@mcp.tool('list_tags_for_resource')
@handle_exceptions
def list_tags_for_resource(
    resource_arn: str,
) -> Dict[str, Any]:
    """Lists the tags associated with a Kinesis data stream.

    Args:
        resource_arn: ARN of the resource to list tags for

    Returns:
        Dictionary containing the tags associated with the stream
    """
    # Validate stream_arn
    if not resource_arn:
        raise ValueError('stream_arn is required')

    if not isinstance(resource_arn, str):
        raise TypeError('resource_arn must be a string')

    if len(resource_arn) < MIN_STREAM_ARN_LENGTH or len(resource_arn) > MAX_STREAM_ARN_LENGTH:
        raise ValueError(
            f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
        )

    # Build parameters
    params: ListTagsForResourceInput = {'ResourceARN': resource_arn}

    # Call Kinesis API to list the tags for the stream
    kinesis = get_kinesis_client()
    response = kinesis.list_tags_for_resource(**params)

    return response


@mcp.tool('describe_limits')
@handle_exceptions
def describe_limits(
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes the limits for a Kinesis data stream in the specified region.

    Args:
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the limits for the Kinesis data stream
    """
    # Build parameters
    params = {}

    # Call Kinesis API to describe the limits
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_limits(**params)

    return response


@mcp.tool('enable_enhanced_monitoring')
@handle_exceptions
def enable_enhanced_monitoring(
    shard_level_metrics: List[str],
    stream_name: str = None,
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Enables enhanced monitoring for a Kinesis data stream.

    Args:
        shard_level_metrics: List of metrics to enable for enhanced monitoring
        stream_name: Name of the stream to enable monitoring for
        stream_arn: ARN of the stream to enable monitoring for
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the response from the Kinesis API
    """
    # Validate shard_level_metrics
    if not shard_level_metrics:
        raise ValueError('shard_level_metrics is required')

    if not isinstance(shard_level_metrics, list):
        raise TypeError('shard_level_metrics must be a list')

    if (
        len(shard_level_metrics) < MIN_SHARD_LEVEL_METRICS
        or len(shard_level_metrics) > MAX_SHARD_LEVEL_METRICS
    ):
        raise ValueError(
            f'Number of shard_level_metrics must be between {MIN_SHARD_LEVEL_METRICS} and {MAX_SHARD_LEVEL_METRICS}'
        )
    for _ in shard_level_metrics:
        if _ not in VALID_SHARD_LEVEL_METRICS:
            raise ValueError(
                f'Invalid metric in shard_level_metrics: {_}. Must be one of {VALID_SHARD_LEVEL_METRICS}'
            )

    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Build parameters
    params: EnableEnhancedMonitoringInput = {'ShardLevelMetrics': shard_level_metrics}

    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to enable enhanced monitoring
    kinesis = get_kinesis_client(region_name)
    response = kinesis.enable_enhanced_monitoring(**params)

    return response


@mcp.tool('get_resource_policy')
@handle_exceptions
def get_resource_policy(
    resource_arn: str,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Retrieves the resource policy for a Kinesis data stream.

    Args:
        resource_arn: ARN of the resource to retrieve the policy for
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the resource policy
    """
    # Validate resource_arn
    if not resource_arn:
        raise ValueError('resource_arn is required')

    if not isinstance(resource_arn, str):
        raise TypeError('resource_arn must be a string')

    if len(resource_arn) < MIN_STREAM_ARN_LENGTH or len(resource_arn) > MAX_STREAM_ARN_LENGTH:
        raise ValueError(
            f'resource_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
        )

    # Build parameters
    params: GetResourcePolicyInput = {'ResourceARN': resource_arn}

    # Call Kinesis API to get the resource policy
    kinesis = get_kinesis_client(region_name)
    response = kinesis.get_resource_policy(**params)

    return response


@mcp.tool('increase_stream_retention_period')
@handle_exceptions
def increase_stream_retention_period(
    retention_period_hours: int,
    stream_name: str = None,
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Increases the retention period of a Kinesis data stream.

    Args:
        retention_period_hours: New retention period in hours (must be between 24 and 8760)
        stream_name: Name of the stream to increase retention for
        stream_arn: ARN of the stream to increase retention for
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the response from the Kinesis API
    """
    # Validate retention_period_hours
    if not isinstance(retention_period_hours, int):
        raise TypeError('retention_period_hours must be an integer')

    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Get current retention period to ensure we're only increasing it
    kinesis = get_kinesis_client(region_name)
    describe_params = {}
    if stream_name is not None:
        describe_params['StreamName'] = stream_name
    if stream_arn is not None:
        describe_params['StreamARN'] = stream_arn
    describe_response = kinesis.describe_stream(**describe_params)
    current_retention_period = describe_response['StreamDescription']['RetentionPeriodHours']

    # Validate retention_period_hours
    if retention_period_hours <= current_retention_period:
        raise ValueError(
            f'retention_period_hours must be greater than the current retention period of {current_retention_period} hours'
        )

    # Build parameters
    params: IncreaseStreamRetentionPeriodInput = {
        'RetentionPeriodHours': retention_period_hours,
    }

    if stream_name is not None:
        params['StreamName'] = stream_name
    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to increase the stream retention period
    response = kinesis.increase_stream_retention_period(**params)

    return response


@mcp.tool('list_shards')
@handle_exceptions
def list_shards(
    exclusive_start_shard_id: str = None,
    stream_name: str = None,
    stream_arn: str = None,
    next_token: str = None,
    max_results: int = DEFAULT_MAX_RESULTS,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the shards in a Kinesis data stream.

    Args:
        exclusive_start_shard_id: Shard ID to start listing from (default: None)
        stream_name: Name of the stream to list shards for
        stream_arn: ARN of the stream to list shards for
        next_token: Token for pagination (default: None)
        max_results: Maximum number of shards to return (default: 100)
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the list of shards and pagination details
    """
    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Validate exclusive_start_shard_id if provided
    if exclusive_start_shard_id is not None:
        if not isinstance(exclusive_start_shard_id, str):
            raise TypeError('exclusive_start_shard_id must be a string')

        if not re.match(r'^[a-zA-Z0-9_.-]+$', exclusive_start_shard_id):
            raise ValueError(
                'exclusive_start_shard_id can only contain alphanumeric characters, underscores, periods, and hyphens'
            )
        if (
            len(exclusive_start_shard_id) < MIN_SHARD_ID_LENGTH
            or len(exclusive_start_shard_id) > MAX_SHARD_ID_LENGTH
        ):
            raise ValueError(
                f'exclusive_start_shard_id length must be between {MIN_SHARD_ID_LENGTH} and {MAX_SHARD_ID_LENGTH} characters'
            )

    # Validate next_token if provided
    if next_token is not None:
        if not isinstance(next_token, str):
            raise TypeError('next_token must be a string')

    # Validate max_results
    if max_results is not None:
        if not isinstance(max_results, int):
            raise TypeError('max_results must be an integer')

        if max_results < MIN_RESULTS_PER_STREAM or max_results > MAX_RESULTS_PER_STREAM:
            raise ValueError(
                f'max_results must be between {MIN_RESULTS_PER_STREAM} and {MAX_RESULTS_PER_STREAM}'
            )

    # Build parameters
    params: ListShardsInput = {}

    if exclusive_start_shard_id is not None:
        params['ExclusiveStartShardId'] = exclusive_start_shard_id

    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    if next_token is not None:
        params['NextToken'] = next_token

    if max_results is not None:
        params['MaxResults'] = max_results

    # Call Kinesis API to list the shards
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_shards(**params)

    return response


@mcp.tool('tag_resource')
@handle_exceptions
def tag_resource(
    resource_arn: str,
    tags: Dict[str, str],
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Adds tags to a Kinesis data stream.

    Args:
        resource_arn: ARN of the resource to add tags to
        tags: Dictionary of tags to add to the resource
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the response from the Kinesis API
    """
    # Validate resource_arn
    if not resource_arn:
        raise ValueError('resource_arn is required')

    if not isinstance(resource_arn, str):
        raise TypeError('resource_arn must be a string')

    if len(resource_arn) < MIN_STREAM_ARN_LENGTH or len(resource_arn) > MAX_STREAM_ARN_LENGTH:
        raise ValueError(
            f'resource_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
        )

    # Validate tags
    if not isinstance(tags, dict):
        raise TypeError('tags must be a dictionary')

    # Build parameters
    params: TagResourceInput = {
        'ResourceARN': resource_arn,
        'Tags': tags,
    }

    # Call Kinesis API to add tags to the resource
    kinesis = get_kinesis_client(region_name)
    response = kinesis.tag_resource(**params)

    return response


@mcp.tool('list_tags_for_stream')
@handle_exceptions
def list_tags_for_stream(
    exclusive_start_tag_key: str = None,
    stream_name: str = None,
    stream_arn: str = None,
    limit: int = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the tags associated with a Kinesis data stream.

    Args:
        exclusive_start_tag_key: Tag key to start listing from (default: None)
        stream_name: Name of the stream to list tags for
        stream_arn: ARN of the stream to list tags for
        limit: Maximum number of tags to return (default: None)
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the list of tags and pagination details
    """
    # Validate stream identification
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if not re.match(r'^[a-zA-Z0-9._-]+$', stream_name):
            raise ValueError(
                'stream_name can only contain alphanumeric characters, hyphens, underscores, and periods'
            )

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

    # Validate exclusive_start_tag_key if provided
    if exclusive_start_tag_key is not None:
        if not isinstance(exclusive_start_tag_key, str):
            raise TypeError('exclusive_start_tag_key must be a string')

    # Validate limit if provided
    if limit is not None:
        if not isinstance(limit, int):
            raise TypeError('limit must be an integer')

        if limit < MIN_LIMIT or limit > MAX_LIMIT:
            raise ValueError(f'limit must be between {MIN_LIMIT} and {MAX_LIMIT}')

    # Build parameters
    params: ListTagsForStreamInput = {}
    if exclusive_start_tag_key is not None:
        params['ExclusiveStartTagKey'] = exclusive_start_tag_key
    if stream_name is not None:
        params['StreamName'] = stream_name
    if stream_arn is not None:
        params['StreamARN'] = stream_arn
    if limit is not None:
        params['Limit'] = limit

    # Call Kinesis API to list the tags for the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_tags_for_stream(**params)

    return response


@mcp.tool('put_resource_policy')
@handle_exceptions
def put_resource_policy(
    resource_arn: str,
    policy: str,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Attaches a resource policy to a Kinesis data stream.

    Args:
        resource_arn: ARN of the resource to attach the policy to
        policy: JSON policy document as a string
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the response from the Kinesis API
    """
    # Validate resource_arn
    if not resource_arn:
        raise ValueError('resource_arn is required')

    if not isinstance(resource_arn, str):
        raise TypeError('resource_arn must be a string')

    if len(resource_arn) < MIN_STREAM_ARN_LENGTH or len(resource_arn) > MAX_STREAM_ARN_LENGTH:
        raise ValueError(
            f'resource_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
        )

    # Validate policy
    if not policy:
        raise ValueError('policy is required')

    if not isinstance(policy, str):
        raise TypeError('policy must be a string')

    # Build parameters
    params: PutResourcePolicyInput = {
        'ResourceARN': resource_arn,
        'Policy': policy,
    }

    # Call Kinesis API to attach the resource policy
    kinesis = get_kinesis_client(region_name)
    response = kinesis.put_resource_policy(**params)

    return response


def main():
    """Run the MCP server with CLI argument support."""
    mcp.run()


if __name__ == '__main__':
    main()
