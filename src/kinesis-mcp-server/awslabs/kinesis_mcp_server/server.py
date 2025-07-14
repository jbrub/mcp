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
from awslabs.kinesis_mcp_server.common import (
    AddTagsToStreamInput,
    CreateStreamInput,
    DecreaseStreamRetentionPeriodInput,
    DeleteResourcePolicyInput,
    DeleteStreamInput,
    DeregisterStreamConsumerInput,
    DescribeStreamConsumerInput,
    DescribeStreamInput,
    DescribeStreamSummaryInput,
    DisableEnhancedMonitoringInput,
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
    MergeShardsInput,
    PutRecordInput,
    PutRecordsInput,
    PutResourcePolicyInput,
    RegisterStreamConsumerInput,
    RemoveTagsFromStreamInput,
    SplitShardInput,
    StartStreamEncryptionInput,
    StopStreamEncryptionInput,
    TagResourceInput,
    UntagResourceInput,
    UpdateShardCountInput,
    UpdateStreamModeInput,
    handle_exceptions,
    mutation_check,
)
from awslabs.kinesis_mcp_server.consts import (
    DEFAULT_GET_RECORDS_LIMIT,
    DEFAULT_MAX_RESULTS,
    # Defaults
    DEFAULT_REGION,
    DEFAULT_SHARD_COUNT,
    DEFAULT_STREAM_LIMIT,
    MAX_RESULTS_PER_STREAM,
    # Shared constants
    STREAM_MODE_ON_DEMAND,
    STREAM_MODE_PROVISIONED,
)
from botocore.config import Config
from datetime import datetime
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from typing import Any, Dict, List, Optional, Union


# MCP Server Set Up
mcp = FastMCP(
    'awslabs.kinesis-mcp-server',
    instructions="""
    This Kinesis MCP server provides tools to interact with Amazon Kinesis Data Streams.

    When using these tools, please specify all relevant parameters explicitly, even when using default values.
    For example, when creating a stream, include the region_name parameter even if using the default region.

    The default region being used is 'us-west-2'. A region must be explicitly stated to use any other region.

    This helps ensure clarity and prevents region-related issues when working with AWS resources.

    If a tool returns an error response (containing an 'error' key in the response), DO NOT proceed with
    subsequent tool calls in your plan. Instead, return the error to the user and wait for further instructions.
    This prevents cascading failures and unintended side effects when operations fail.

    Error responses are structured with an 'error' key containing a descriptive message about what went wrong.
    These structured errors allow for programmatic handling and clear communication of issues to users.
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
@mutation_check
async def put_records(
    records: List[Dict[str, Any]] = Field(
        ...,
        description='List of records to write to the stream, in the format: List[Dict[str, Any]]',
    ),
    stream_name: Optional[str] = Field(
        default=None, description='The name of the stream to write to'
    ),
    stream_arn: Optional[str] = Field(default=None, description='ARN of the stream to write to'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Writes multiple data records to a Kinesis data stream in a single call."""
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
    return {
        'message': 'Successfully wrote records to Kinesis stream',
        'status': 'success' if response.get('FailedRecordCount', 0) == 0 else 'partial_success',
        'stream_identifier': stream_name or stream_arn,
        'total_records': len(records),
        'successful_records': len(records) - response.get('FailedRecordCount', 0),
        'failed_records': response.get('FailedRecordCount', 0),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('get_records')
@handle_exceptions
async def get_records(
    shard_iterator: str = Field(
        ...,
        description='The shard iterator to use for retrieving records - use get_shard_iterator to obtain this',
    ),
    limit: int = Field(
        default=DEFAULT_GET_RECORDS_LIMIT,
        description='Maximum number of records to retrieve (default: 10000)',
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to retrieve records from'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Retrieves records from a Kinesis shard."""
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
    return {
        'message': 'Successfully retrieved records from Kinesis shard',
        'status': 'success',
        'record_count': len(response.get('Records', [])),
        'millis_behind_latest': response.get('MillisBehindLatest', 0),
        'next_shard_iterator': response.get('NextShardIterator'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('create_stream')
@handle_exceptions
@mutation_check
async def create_stream(
    stream_name: str = Field(
        ...,
        description='stream_name: A name to identify the stream, must follow Kinesis naming conventions',
    ),
    shard_count: Optional[int] = Field(
        default=None,
        description='shard_count: Number of shards to create (default: 1), only used if stream_mode_details is set to PROVISIONED',
    ),
    stream_mode_details: Dict[str, str] = Field(
        default={'StreamMode': STREAM_MODE_ON_DEMAND},
        description='stream_mode_details: Details about the stream mode (default: {"StreamMode": "ON_DEMAND"})',
    ),
    tags: Optional[Dict[str, str]] = Field(
        default=None, description='tags: Tags to associate with the stream'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Creates a new Kinesis data stream with the specified name and shard count."""
    # Build parameters
    params: CreateStreamInput = {'StreamName': stream_name}

    # Use default stream mode if stream_mode_details is not a dict
    if not isinstance(stream_mode_details, dict):
        stream_mode_details = {'StreamMode': STREAM_MODE_ON_DEMAND}

    params['StreamModeDetails'] = stream_mode_details

    # Add ShardCount only for PROVISIONED mode
    stream_mode = stream_mode_details.get('StreamMode', STREAM_MODE_ON_DEMAND)
    if stream_mode == STREAM_MODE_PROVISIONED:
        params['ShardCount'] = DEFAULT_SHARD_COUNT if shard_count is None else shard_count

    # Add tags if provided
    if tags:
        params['Tags'] = tags

    # Call Kinesis API to create the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.create_stream(**params)

    return {
        'message': f"Successfully created Kinesis stream '{stream_name}'",
        'status': 'success',
        'stream_name': stream_name,
        'shard_count': shard_count,
        'stream_mode': params['StreamModeDetails']['StreamMode'],
        'tags': tags if tags else {},
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('list_streams')
@handle_exceptions
async def list_streams(
    exclusive_start_stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to start listing from (default: None)'
    ),
    limit: int = Field(
        default=DEFAULT_STREAM_LIMIT,
        description='Maximum number of streams to list (default: 100)',
    ),
    next_token: Optional[str] = Field(
        default=None, description='Token for pagination (default: None)'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the Kinesis data streams."""
    # Initialize parameters
    params: ListStreamsInput = {}

    # Add optional parameters
    if exclusive_start_stream_name is not None:
        params['ExclusiveStartStreamName'] = exclusive_start_stream_name

    if limit is not None:
        params['Limit'] = limit

    if next_token is not None:
        params['NextToken'] = next_token

    # Call Kinesis API to list the streams
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_streams(**params)

    return {
        'StreamNames': response.get('StreamNames', []),
        'HasMoreStreams': response.get('HasMoreStreams', False),
        'NextToken': response.get('NextToken', None),
        'StreamSummaries': response.get('StreamSummaries', []),
        'api_response': response,
    }


@mcp.tool('describe_stream_summary')
@handle_exceptions
async def describe_stream_summary(
    stream_name: Optional[str] = Field(default=None, description='Name of the stream to describe'),
    stream_arn: Optional[str] = Field(default=None, description='ARN of the stream to describe'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Returns a description summary of a stream."""
    # Initialize parameters
    params: DescribeStreamSummaryInput = {}

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Validate that at least one identifier is provided
    if 'StreamName' not in params and 'StreamARN' not in params:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Call Kinesis API to describe the stream summary
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_stream_summary(**params)

    # Return Stream Summary Details
    return {
        'message': 'Successfully retrieved stream summary',
        'status': 'success',
        'stream_name': response.get('StreamName'),
        'stream_arn': response.get('StreamARN'),
        'stream_status': response.get('StreamStatus'),
        'retention_period_hours': response.get('RetentionPeriodHours'),
        'stream_creation_timestamp': response.get('StreamCreationTimestamp'),
        'shard_count': response.get('OpenShardCount'),
        'stream_mode': response.get('StreamModeDetails', {}).get('StreamMode')
        if response.get('StreamModeDetails')
        else None,
        'encryption_type': response.get('EncryptionType'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('get_shard_iterator')
@handle_exceptions
async def get_shard_iterator(
    shard_id: str = Field(..., description='Shard ID to retrieve the iterator for'),
    shard_iterator_type: str = Field(
        ...,
        description='Type of shard iterator to retrieve (AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST, AT_TIMESTAMP)',
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to retrieve the shard iterator for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to retrieve the shard iterator for'
    ),
    starting_sequence_number: Optional[str] = Field(
        default=None,
        description='Sequence number to start retrieving records from (required for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER)',
    ),
    timestamp: Optional[Union[datetime, str]] = Field(
        default=None,
        description='Timestamp to start retrieving records from (required for AT_TIMESTAMP)',
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Retrieves a shard iterator for a specified shard."""
    # Build parameters
    params: GetShardIteratorInput = {
        'ShardId': shard_id,
        'ShardIteratorType': shard_iterator_type,
    }

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Add optional parameters for specific iterator types
    if shard_iterator_type in ['AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER']:
        if starting_sequence_number is None:
            raise ValueError(
                'starting_sequence_number is required for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER shard iterator types'
            )
        params['StartingSequenceNumber'] = starting_sequence_number

    if shard_iterator_type == 'AT_TIMESTAMP':
        if timestamp is None:
            raise ValueError('timestamp is required for AT_TIMESTAMP shard iterator type')
        params['Timestamp'] = timestamp

    # Call Kinesis API to get the shard iterator
    kinesis = get_kinesis_client(region_name)
    response = kinesis.get_shard_iterator(**params)

    return {
        'message': 'Successfully retrieved shard iterator',
        'status': 'success',
        'shard_iterator': response.get('ShardIterator'),
        'shard_id': shard_id,
        'stream_identifier': stream_name or stream_arn,
        'iterator_type': shard_iterator_type,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('add_tags_to_stream')
@handle_exceptions
@mutation_check
async def add_tags_to_stream(
    tags: Dict[str, str] = Field(..., description='Tags to associate with the stream'),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to add tags to'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to add tags to'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Adds tags to a Kinesis data stream."""
    # Build parameters
    params: AddTagsToStreamInput = {'Tags': tags}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to add tags to the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.add_tags_to_stream(**params)

    return {
        'message': 'Successfully added tags to stream',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'tags': tags,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('describe_stream')
@handle_exceptions
async def describe_stream(
    stream_name: Optional[str] = Field(default=None, description='Name of the stream to describe'),
    stream_arn: Optional[str] = Field(default=None, description='ARN of the stream to describe'),
    limit: int = Field(
        default=DEFAULT_STREAM_LIMIT,
        description='Maximum number of shards to return (default: 100)',
    ),
    exclusive_start_shard_id: Optional[str] = Field(
        default=None, description='Shard ID to start listing from'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes the specified stream."""
    # Initialize parameters
    params: DescribeStreamInput = {}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add parameters
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
    return {
        'message': 'Successfully described stream',
        'status': 'success',
        'stream_name': response.get('StreamName'),
        'stream_arn': response.get('StreamARN'),
        'stream_status': response.get('StreamStatus'),
        'shards': response.get('Shards', []),
        'has_more_shards': response.get('HasMoreShards', False),
        'retention_period_hours': response.get('RetentionPeriodHours'),
        'stream_creation_timestamp': response.get('StreamCreationTimestamp'),
        'enhanced_monitoring': response.get('EnhancedMonitoring', []),
        'encryption_type': response.get('EncryptionType'),
        'key_id': response.get('KeyId'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('describe_stream_consumer')
@handle_exceptions
async def describe_stream_consumer(
    consumer_name: Optional[str] = Field(
        default=None, description='Name of the consumer to describe'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream the consumer belongs to'
    ),
    consumer_arn: Optional[str] = Field(
        default=None, description='ARN of the consumer to describe'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes a Kinesis data stream consumer."""
    # Validate consumer identification
    if consumer_arn is None and (consumer_name is None or stream_arn is None):
        raise ValueError(
            'Either consumer_arn or both consumer_name and stream_arn must be provided'
        )

    # Build parameters
    params: DescribeStreamConsumerInput = {}

    # Add parameters - only add if not None
    if consumer_arn is not None:
        params['ConsumerARN'] = consumer_arn
    elif consumer_name is not None and stream_arn is not None:
        params['ConsumerName'] = consumer_name
        params['StreamARN'] = stream_arn

    # Call Kinesis API to describe the stream consumer
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_stream_consumer(**params)

    # Return Stream Details
    return {
        'message': 'Successfully described stream consumer',
        'status': 'success',
        'consumer_name': response.get('ConsumerDescription', {}).get('ConsumerName'),
        'consumer_arn': response.get('ConsumerDescription', {}).get('ConsumerARN'),
        'consumer_status': response.get('ConsumerDescription', {}).get('ConsumerStatus'),
        'consumer_creation_timestamp': response.get('ConsumerDescription', {}).get(
            'ConsumerCreationTimestamp'
        ),
        'stream_arn': response.get('ConsumerDescription', {}).get('StreamARN'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('list_stream_consumers')
@handle_exceptions
async def list_stream_consumers(
    stream_arn: str = Field(..., description='ARN of the stream to list consumers for'),
    next_token: Optional[str] = Field(
        default=None, description='Token for pagination (default: None)'
    ),
    stream_creation_time_stamp: Optional[Union[datetime, str]] = Field(
        default=None, description='Timestamp to filter consumers created after this time'
    ),
    max_results: int = Field(
        default=DEFAULT_MAX_RESULTS,
        description='Maximum number of results to return (default: 100)',
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the consumers of a Kinesis data stream."""
    # Build parameters
    params: ListStreamConsumersInput = {'StreamARN': stream_arn}

    # Add optional parameters
    if next_token is not None:
        params['NextToken'] = next_token

    if stream_creation_time_stamp is not None:
        params['StreamCreationTimestamp'] = stream_creation_time_stamp

    if max_results is not None:
        params['MaxResults'] = max_results

    # Call Kinesis API to list the stream consumers
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_stream_consumers(**params)

    return {
        'message': 'Successfully listed stream consumers',
        'status': 'success',
        'stream_arn': stream_arn,
        'consumer_count': len(response.get('Consumers', [])),
        'consumers': response.get('Consumers', []),
        'next_token': response.get('NextToken'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('list_tags_for_resource')
@handle_exceptions
async def list_tags_for_resource(
    resource_arn: str = Field(..., description='ARN of the resource to list tags for'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the tags associated with a Kinesis data stream."""
    # Build parameters
    params: ListTagsForResourceInput = {'ResourceARN': resource_arn}

    # Call Kinesis API to list the tags for the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_tags_for_resource(**params)

    return {
        'message': 'Successfully listed tags for resource',
        'status': 'success',
        'resource_arn': resource_arn,
        'tags': response.get('Tags', {}),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('describe_limits')
@handle_exceptions
async def describe_limits(
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Describes the limits for a Kinesis data stream in the specified region."""
    # Call Kinesis API to describe the limits
    kinesis = get_kinesis_client(region_name)
    response = kinesis.describe_limits()

    return {
        'message': 'Successfully retrieved Kinesis limits',
        'status': 'success',
        'shard_limit': response.get('ShardLimit'),
        'open_shard_count': response.get('OpenShardCount'),
        'on_demand_stream_count': response.get('OnDemandStreamCount'),
        'on_demand_stream_count_limit': response.get('OnDemandStreamCountLimit'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('enable_enhanced_monitoring')
@handle_exceptions
@mutation_check
async def enable_enhanced_monitoring(
    shard_level_metrics: List[str] = Field(
        ..., description='List of metrics to enable for enhanced monitoring'
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to enable monitoring for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to enable monitoring for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Enables enhanced monitoring for a Kinesis data stream."""
    # Build parameters
    params: EnableEnhancedMonitoringInput = {'ShardLevelMetrics': shard_level_metrics}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to enable enhanced monitoring
    kinesis = get_kinesis_client(region_name)
    response = kinesis.enable_enhanced_monitoring(**params)

    return {
        'message': 'Successfully enabled enhanced monitoring',
        'current_shard_level_metrics': response.get('CurrentShardLevelMetrics', []),
        'desired_shard_level_metrics': response.get('DesiredShardLevelMetrics', []),
        'api_response': response,
    }


@mcp.tool('get_resource_policy')
@handle_exceptions
async def get_resource_policy(
    resource_arn: str = Field(..., description='ARN of the resource to retrieve the policy for'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Retrieves the resource policy for a Kinesis data stream."""
    # Build parameters
    params: GetResourcePolicyInput = {'ResourceARN': resource_arn}

    # Call Kinesis API to get the resource policy
    kinesis = get_kinesis_client(region_name)
    response = kinesis.get_resource_policy(**params)

    return {
        'message': 'Successfully retrieved resource policy',
        'status': 'success',
        'resource_arn': resource_arn,
        'policy': response.get('Policy'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('increase_stream_retention_period')
@handle_exceptions
@mutation_check
async def increase_stream_retention_period(
    retention_period_hours: int = Field(
        ..., description='New retention period in hours (must be between 24 and 8760)'
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to increase retention for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to increase retention for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Increases the retention period of a Kinesis data stream."""
    # Build parameters
    params: IncreaseStreamRetentionPeriodInput = {'RetentionPeriodHours': retention_period_hours}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to increase the stream retention period
    kinesis = get_kinesis_client(region_name)
    response = kinesis.increase_stream_retention_period(**params)

    return response


@mcp.tool('list_shards')
@handle_exceptions
async def list_shards(
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to list shards for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to list shards for'
    ),
    exclusive_start_shard_id: Optional[str] = Field(
        default=None, description='Shard ID to start listing from'
    ),
    next_token: Optional[str] = Field(default=None, description='Token for pagination'),
    max_results: int = Field(
        default=MAX_RESULTS_PER_STREAM,
        description='Maximum number of shards to return (default: 1000)',
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the shards in a Kinesis data stream."""
    # Build parameters
    params: ListShardsInput = {}

    # Validate that at least one stream identifier is provided if next_token is not provided
    if next_token is None and stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name, stream_arn, or next_token must be provided')

    # Add parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    if exclusive_start_shard_id is not None:
        params['ExclusiveStartShardId'] = exclusive_start_shard_id

    if next_token is not None:
        params['NextToken'] = next_token

    if max_results is not None:
        params['MaxResults'] = max_results

    # Call Kinesis API to list the shards
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_shards(**params)

    return {
        'message': 'Successfully listed shards',
        'shard_count': len(response.get('Shards', [])),
        'shards': response.get('Shards', []),
        'next_token': response.get('NextToken'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('tag_resource')
@handle_exceptions
@mutation_check
async def tag_resource(
    resource_arn: str = Field(..., description='ARN of the resource to add tags to'),
    tags: Dict[str, str] = Field(..., description='Dictionary of tags to add to the resource'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Adds tags to a Kinesis resource."""
    # Build parameters
    params: TagResourceInput = {'ResourceARN': resource_arn, 'Tags': tags}

    # Call Kinesis API to tag the resource
    kinesis = get_kinesis_client(region_name)
    response = kinesis.tag_resource(**params)

    return {
        'message': 'Successfully tagged resource',
        'status': 'success',
        'resource_arn': resource_arn,
        'tags': tags,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('list_tags_for_stream')
@handle_exceptions
async def list_tags_for_stream(
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to list tags for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to list tags for'
    ),
    exclusive_start_tag_key: Optional[str] = Field(
        default=None, description='Key to start listing from (for pagination)'
    ),
    limit: Optional[int] = Field(default=None, description='Maximum number of tags to return'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Lists the tags associated with a Kinesis data stream."""
    # Build parameters
    params: ListTagsForStreamInput = {}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Add optional parameters
    if exclusive_start_tag_key is not None:
        params['ExclusiveStartTagKey'] = exclusive_start_tag_key

    if limit is not None:
        params['Limit'] = limit

    # Call Kinesis API to list the tags for the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_tags_for_stream(**params)

    return {
        'message': 'Successfully listed tags for stream',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'tags': response.get('Tags', {}),
        'has_more_tags': response.get('HasMoreTags', False),
        'region': region_name,
    }


@mcp.tool('put_resource_policy')
@handle_exceptions
@mutation_check
async def put_resource_policy(
    resource_arn: str = Field(..., description='ARN of the resource to attach the policy to'),
    policy: str = Field(..., description='JSON policy document as a string'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Attaches a resource policy to a Kinesis data stream."""
    # Build parameters
    params: PutResourcePolicyInput = {'ResourceARN': resource_arn, 'Policy': policy}

    # Call Kinesis API to attach the resource policy
    kinesis = get_kinesis_client(region_name)
    response = kinesis.put_resource_policy(**params)

    return {
        'message': 'Successfully attached resource policy',
        'status': 'success',
        'resource_arn': resource_arn,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('delete_stream')
@handle_exceptions
@mutation_check
async def delete_stream(
    stream_name: Optional[str] = Field(default=None, description='Name of the stream to delete'),
    stream_arn: Optional[str] = Field(default=None, description='ARN of the stream to delete'),
    enforce_consumer_deletion: Optional[bool] = Field(
        default=None, description='Whether to enforce deletion of consumers'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Deletes a Kinesis data stream."""
    # Build parameters
    params: DeleteStreamInput = {}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Add optional parameters
    if enforce_consumer_deletion is not None:
        params['EnforceConsumerDeletion'] = enforce_consumer_deletion

    # Call Kinesis API to delete the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.delete_stream(**params)

    return {
        'message': 'Successfully deleted stream',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'enforce_consumer_deletion': enforce_consumer_deletion,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('decrease_stream_retention_period')
@handle_exceptions
@mutation_check
async def decrease_stream_retention_period(
    retention_period_hours: int = Field(
        ..., description='New retention period in hours (must be between 24 and 8760)'
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to decrease retention for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to decrease retention for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Decreases the retention period of a Kinesis data stream."""
    # Build parameters
    params: DecreaseStreamRetentionPeriodInput = {'RetentionPeriodHours': retention_period_hours}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to decrease the stream retention period
    kinesis = get_kinesis_client(region_name)
    response = kinesis.decrease_stream_retention_period(**params)

    return {
        'message': f'Successfully decreased stream retention period to {retention_period_hours} hours',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'retention_period_hours': retention_period_hours,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('delete_resource_policy')
@handle_exceptions
@mutation_check
async def delete_resource_policy(
    resource_arn: str = Field(..., description='ARN of the resource to delete the policy for'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Deletes the resource policy for a Kinesis data stream."""
    # Build parameters
    params: DeleteResourcePolicyInput = {'ResourceARN': resource_arn}

    # Call Kinesis API to delete the resource policy
    kinesis = get_kinesis_client(region_name)
    response = kinesis.delete_resource_policy(**params)

    return {
        'message': 'Successfully deleted resource policy',
        'status': 'success',
        'resource_arn': resource_arn,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('deregister_stream_consumer')
@handle_exceptions
@mutation_check
async def deregister_stream_consumer(
    consumer_name: Optional[str] = Field(
        default=None, description='Name of the consumer to deregister'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream the consumer belongs to'
    ),
    consumer_arn: Optional[str] = Field(
        default=None, description='ARN of the consumer to deregister'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Deregisters a consumer from a Kinesis data stream."""
    # Build parameters
    params: DeregisterStreamConsumerInput = {}

    # Validate that at least one consumer identifier is provided
    if consumer_name is None and consumer_arn is None:
        raise ValueError('Either consumer_name or consumer_arn must be provided')

    # Add consumer identifier parameters
    if consumer_name is not None:
        params['ConsumerName'] = consumer_name

    if consumer_arn is not None:
        params['ConsumerARN'] = consumer_arn

    # Add stream ARN parameter if provided
    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to deregister the stream consumer
    kinesis = get_kinesis_client(region_name)
    response = kinesis.deregister_stream_consumer(**params)

    return {
        'message': 'Successfully deregistered stream consumer',
        'status': 'success',
        'consumer_identifier': consumer_name or consumer_arn,
        'stream_arn': stream_arn,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('disable_enhanced_monitoring')
@handle_exceptions
@mutation_check
async def disable_enhanced_monitoring(
    shard_level_metrics: List[str] = Field(
        ..., description='List of metrics to disable for enhanced monitoring'
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to disable monitoring for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to disable monitoring for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Disables enhanced monitoring for a Kinesis data stream."""
    # Build parameters
    params: DisableEnhancedMonitoringInput = {'ShardLevelMetrics': shard_level_metrics}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to disable enhanced monitoring
    kinesis = get_kinesis_client(region_name)
    response = kinesis.disable_enhanced_monitoring(**params)

    return {
        'message': 'Successfully disabled enhanced monitoring',
        'status': 'success',
        'current_shard_level_metrics': response.get('CurrentShardLevelMetrics', []),
        'desired_shard_level_metrics': response.get('DesiredShardLevelMetrics', []),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('merge_shards')
@handle_exceptions
@mutation_check
async def merge_shards(
    shard_to_merge: str = Field(..., description='Shard ID of the shard to merge'),
    adjacent_shard_to_merge: str = Field(
        ..., description='Shard ID of the adjacent shard to merge'
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to merge shards in'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to merge shards in'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Merges two adjacent shards in a Kinesis data stream."""
    # Build parameters
    params: MergeShardsInput = {
        'ShardToMerge': shard_to_merge,
        'AdjacentShardToMerge': adjacent_shard_to_merge,
    }

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to merge the shards
    kinesis = get_kinesis_client(region_name)
    response = kinesis.merge_shards(**params)

    return {
        'message': 'Successfully merged shards',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'shard_to_merge': shard_to_merge,
        'adjacent_shard_to_merge': adjacent_shard_to_merge,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('remove_tags_from_stream')
@handle_exceptions
@mutation_check
async def remove_tags_from_stream(
    tag_keys: List[str] = Field(..., description='List of tag keys to remove from the stream'),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to remove tags from'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to remove tags from'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Removes tags from a Kinesis data stream."""
    # Build parameters
    params: RemoveTagsFromStreamInput = {'TagKeys': tag_keys}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to remove tags from the stream
    kinesis = get_kinesis_client(region_name)
    response = kinesis.remove_tags_from_stream(**params)

    return {
        'message': 'Successfully removed tags from stream',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'tag_keys': tag_keys,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('split_shard')
@handle_exceptions
@mutation_check
async def split_shard(
    shard_to_split: str = Field(..., description='Shard ID of the shard to split'),
    new_starting_hash_key: str = Field(..., description='New starting hash key for the new shard'),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to split the shard in'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to split the shard in'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Splits a shard into two shards in a Kinesis data stream."""
    # Build parameters
    params: SplitShardInput = {
        'ShardToSplit': shard_to_split,
        'NewStartingHashKey': new_starting_hash_key,
    }

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to split the shard
    kinesis = get_kinesis_client(region_name)
    response = kinesis.split_shard(**params)

    return {
        'message': 'Successfully split shard',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'shard_to_split': shard_to_split,
        'new_starting_hash_key': new_starting_hash_key,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('start_stream_encryption')
@mutation_check
@mutation_check
async def start_stream_encryption(
    key_id: str = Field(..., description='ARN or alias of the KMS key to use for encryption'),
    encryption_type: str = Field(
        default='KMS', description="Type of encryption to use (default: 'KMS')"
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to start encryption for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to start encryption for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Starts encryption for a Kinesis data stream."""
    # Build parameters
    params: StartStreamEncryptionInput = {'KeyId': key_id, 'EncryptionType': encryption_type}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to start stream encryption
    kinesis = get_kinesis_client(region_name)
    response = kinesis.start_stream_encryption(**params)

    return {
        'message': 'Successfully started stream encryption',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'encryption_type': encryption_type,
        'key_id': key_id,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('stop_stream_encryption')
@handle_exceptions
@mutation_check
async def stop_stream_encryption(
    encryption_type: str = Field(
        default='KMS', description="Type of encryption to stop (default: 'KMS')"
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to stop encryption for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to stop encryption for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Stops encryption for a Kinesis data stream."""
    # Build parameters
    params: StopStreamEncryptionInput = {'EncryptionType': encryption_type}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to stop stream encryption
    kinesis = get_kinesis_client(region_name)
    response = kinesis.stop_stream_encryption(**params)

    return {
        'message': 'Successfully stopped stream encryption',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'encryption_type': encryption_type,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('untag_resource')
@handle_exceptions
@mutation_check
async def untag_resource(
    resource_arn: str = Field(..., description='ARN of the resource to remove tags from'),
    tag_keys: List[str] = Field(..., description='List of tag keys to remove'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Removes tags from a Kinesis resource."""
    # Build parameters
    params: UntagResourceInput = {'ResourceARN': resource_arn, 'TagKeys': tag_keys}

    # Call Kinesis API to remove tags from the resource
    kinesis = get_kinesis_client(region_name)
    response = kinesis.untag_resource(**params)

    return {
        'message': 'Successfully removed tags from resource',
        'status': 'success',
        'resource_arn': resource_arn,
        'tag_keys': tag_keys,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('update_shard_count')
@handle_exceptions
@mutation_check
async def update_shard_count(
    target_shard_count: int = Field(..., description='Desired number of shards'),
    scaling_type: str = Field(..., description='Type of scaling (e.g., UNIFORM_SCALING)'),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to update shard count for'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to update shard count for'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Updates the shard count of a Kinesis data stream."""
    # Build parameters
    params: UpdateShardCountInput = {
        'TargetShardCount': target_shard_count,
        'ScalingType': scaling_type,
    }

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to update the shard count
    kinesis = get_kinesis_client(region_name)
    response = kinesis.update_shard_count(**params)

    return {
        'message': f'Successfully updated shard count to {target_shard_count}',
        'status': 'success',
        'target_shard_count': target_shard_count,
        'scaling_type': scaling_type,
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('update_stream_mode')
@handle_exceptions
@mutation_check
async def update_stream_mode(
    stream_mode_details: str = Field(
        ..., description='New mode for the stream (e.g., PROVISIONED, ON_DEMAND)'
    ),
    stream_arn: str = Field(..., description='ARN of the stream to update'),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Updates the mode of a Kinesis data stream."""
    # Build parameters
    params: UpdateStreamModeInput = {
        'StreamARN': stream_arn,
        'StreamModeDetails': {'StreamMode': stream_mode_details},
    }

    # Call Kinesis API to update the stream mode
    kinesis = get_kinesis_client(region_name)
    response = kinesis.update_stream_mode(**params)

    return {
        'message': f'Successfully updated stream mode to {response.get("StreamMode")}',
        'status': 'success',
        'stream_arn': stream_arn,
        'stream_mode': response.get('StreamMode'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('put_record')
@handle_exceptions
@mutation_check
async def put_record(
    data: str = Field(..., description='The data blob to put into the record'),
    partition_key: str = Field(
        ..., description='Determines which shard in the stream the data record is assigned to'
    ),
    stream_name: Optional[str] = Field(
        default=None, description='Name of the stream to put the record into'
    ),
    stream_arn: Optional[str] = Field(
        default=None, description='ARN of the stream to put the record into'
    ),
    explicit_hash_key: Optional[str] = Field(
        default=None,
        description='The hash value used to explicitly determine the shard the data record is assigned to',
    ),
    sequence_number_for_ordering: Optional[str] = Field(
        default=None, description='The sequence number of a record that this record follows'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Writes a single data record into a Kinesis data stream."""
    # Build parameters
    params: PutRecordInput = {'Data': data.encode('utf-8'), 'PartitionKey': partition_key}

    # Validate that at least one stream identifier is provided
    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')

    # Add stream identifier parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Add optional parameters
    if explicit_hash_key is not None:
        params['ExplicitHashKey'] = explicit_hash_key

    if sequence_number_for_ordering is not None:
        params['SequenceNumberForOrdering'] = sequence_number_for_ordering

    # Call Kinesis API to put the record
    kinesis = get_kinesis_client(region_name)
    response = kinesis.put_record(**params)

    return {
        'message': 'Successfully wrote record to Kinesis stream',
        'status': 'success',
        'stream_identifier': stream_name or stream_arn,
        'shard_id': response.get('ShardId'),
        'sequence_number': response.get('SequenceNumber'),
        'encryption_type': response.get('EncryptionType'),
        'region': region_name,
        'api_response': response,
    }


@mcp.tool('register_stream_consumer')
@handle_exceptions
@mutation_check
async def register_stream_consumer(
    stream_arn: str = Field(..., description='ARN of the stream to register the consumer for'),
    consumer_name: str = Field(..., description='Name of the consumer to register'),
    tags: Optional[Dict[str, str]] = Field(
        default=None, description='Tags to associate with the consumer'
    ),
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Registers a consumer with a Kinesis data stream."""
    # Build parameters
    params: RegisterStreamConsumerInput = {
        'StreamARN': stream_arn,
        'ConsumerName': consumer_name,
    }

    if tags is not None:
        params['Tags'] = tags

    # Call Kinesis API to register the consumer
    kinesis = get_kinesis_client(region_name)
    response = kinesis.register_stream_consumer(**params)

    return {
        'message': 'Successfully registered consumer',
        'status': 'success',
        'stream_arn': stream_arn,
        'consumer_name': consumer_name,
        'tags': tags if tags is not None else None,
        'region': region_name,
        'api_response': response,
    }


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == '__main__':
    main()
