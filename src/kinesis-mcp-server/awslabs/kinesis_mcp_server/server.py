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

import boto3
import os
from awslabs.kinesis_mcp_server.common import (
    CreateStreamInput,
    DescribeStreamSummaryInput,
    GetRecordsInput,
    ListStreamsInput,
    PutRecordsInput,
    handle_exceptions,
)
from awslabs.kinesis_mcp_server.consts import (
    DEFAULT_GET_RECORDS_LIMIT,
    # Default Region
    DEFAULT_REGION,
    # Default values
    DEFAULT_SHARD_COUNT,
    DEFAULT_STREAM_LIMIT,
    MAX_LENGTH_SHARD_ITERATOR,
    # get_records constants
    MAX_LIMIT,
    # put_records constants
    MAX_RECORDS,
    MAX_SHARDS_PER_STREAM,
    MAX_STREAM_ARN_LENGTH,
    # Shared constants
    MAX_STREAM_NAME_LENGTH,
    MAX_TAG_KEY_LENGTH,
    MAX_TAG_VALUE_LENGTH,
    # create_stream constants
    MAX_TAGS_COUNT,
    MIN_LENGTH_SHARD_ITERATOR,
    MIN_LIMIT,
    MIN_RECORDS,
    MIN_STREAM_ARN_LENGTH,
    MIN_STREAM_NAME_LENGTH,
    MIN_TAG_KEY_LENGTH,
    STREAM_MODE_ON_DEMAND,
    # Stream modes
    STREAM_MODE_PROVISIONED,
)
from botocore.config import Config
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List


# MCP Server Declaration
mcp = FastMCP(
    'awslabs.kinesis-mcp-server',
    instructions="""
    This Kinesis MCP server provides tools to interact with Amazon Kinesis Data Streams.

    When using these tools, please specify all relevant parameters explicitly, even when using default values.
    For example, when creating a stream, include the region_name parameter even if using the default region.

    The default region being used is 'us-west-1'. A region must be explicitly stated to use any other region.

    This helps ensure clarity and prevents region-related issues when working with AWS resources.
    """,
    dependencies=[
        'pydantic',
        'loguru',
    ],
    version='alpha',
)


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
    shard_count: int = DEFAULT_SHARD_COUNT,
    stream_mode_details: Dict[str, str] = {'StreamMode': STREAM_MODE_ON_DEMAND},
    tags: Dict[str, str] = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Creates a new Kinesis data stream with the specified name and shard count.

    Args:
        stream_name: A name to identify the stream
        shard_count: Number of shards to create (default: 1)
        stream_mode_details: Details about the stream mode (default: {"StreamMode": "PROVISIONED"})
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

    # Validate shard_count
    if not isinstance(shard_count, int):
        raise TypeError('shard_count must be an integer')

    if shard_count < 1 or shard_count > MAX_SHARDS_PER_STREAM:
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
        params['ShardCount'] = shard_count

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
    # Initialize parameters
    params: DescribeStreamSummaryInput = {}

    # Validate that at least one identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Validate stream_name if provided
    if stream_name is not None:
        if not isinstance(stream_name, str):
            raise TypeError('stream_name must be a string')

        if len(stream_name) < MIN_STREAM_NAME_LENGTH or len(stream_name) > MAX_STREAM_NAME_LENGTH:
            raise ValueError(
                f'stream_name length must be between {MIN_STREAM_NAME_LENGTH} and {MAX_STREAM_NAME_LENGTH} characters'
            )

        params['StreamName'] = stream_name

    # Validate stream_arn if provided
    if stream_arn is not None:
        if not isinstance(stream_arn, str):
            raise TypeError('stream_arn must be a string')

        if len(stream_arn) < MIN_STREAM_ARN_LENGTH or len(stream_arn) > MAX_STREAM_ARN_LENGTH:
            raise ValueError(
                f'stream_arn length must be between {MIN_STREAM_ARN_LENGTH} and {MAX_STREAM_ARN_LENGTH} characters'
            )

        params['StreamARN'] = stream_arn

    # Call Kinesis API to describe the stream summary
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_stream_summary(**params)

    # Return Stream Summary Details
    return response


def main():
    """Run the MCP server with CLI argument support."""
    mcp.run()


if __name__ == '__main__':
    main()
