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
    DEFAULT_REGION,
    DEFAULT_SHARD_COUNT,
    DEFAULT_STREAM_LIMIT,
    STREAM_MODE_PROVISIONED,
    STREAM_STATUS_CREATING,
)
from botocore.config import Config
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List


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
    stream_name: str,
    records: List[Dict[str, Any]],
    stream_arn: str = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    """Writes multiple data records to a Kinesis data stream in a single call.

    Args:
        stream_name: The name of the stream to write to
        records: List of records to write to the stream
        stream_arn: ARN of the stream to write to
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        Dictionary containing the sequence number and shard ID of the records
    """
    # Required Paramaters
    params: PutRecordsInput = {'Records': records}

    # Optional Paramaters
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
    limit: int = None,
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
    # Required Paramaters
    params: GetRecordsInput = {'ShardIterator': shard_iterator}

    # Optional Paramaters
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
    stream_mode_details: Dict[str, str] = None,
    tags: Dict[str, str] = None,
    region_name: str = DEFAULT_REGION,
) -> Dict[str, Any]:
    # TODO: There is a limit of MAX_SHARDS_PER_REGION shards per region
    # TODO: Check for formatting of name possibly? Cannot have certain characters
    # TODO: Limit of MAX_TAGS Tags - Test 200
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
    # Required Paramaters
    params: CreateStreamInput = {'StreamName': stream_name}

    # Optional Paramaters
    if stream_mode_details is None:
        stream_mode_details = {'StreamMode': STREAM_MODE_PROVISIONED}

    params['StreamModeDetails'] = stream_mode_details

    # Add ShardCount only for PROVISIONED mode
    if stream_mode_details.get('StreamMode') == STREAM_MODE_PROVISIONED:
        params['ShardCount'] = shard_count

    if tags is not None:
        params['Tags'] = tags

    # Call Kinesis API to create the stream
    kinesis = get_kinesis_client(region_name)
    kinesis.create_stream(**params)

    # Return Status Details of the Stream Creation
    return {
        'StreamName': stream_name,
        'ShardCount': shard_count,
        'StreamModeDetails': stream_mode_details,
        'Tags': tags,
        'Status': STREAM_STATUS_CREATING,
        'Region': region_name,
    }


@mcp.tool('list_streams')
@handle_exceptions
def list_streams(
    limit: int = DEFAULT_STREAM_LIMIT,
    exclusive_start_stream_name: str = None,
    next_token: str = None,
    region_name: str = DEFAULT_REGION,
) -> List[Dict[str, Any]]:
    """Lists the Kinesis data streams.

    Args:
        limit: Maximum number of streams to list (default: 100)
        exclusive_start_stream_name: Name of the stream to start listing from (default: None)
        next_token: Token for pagination (default: None)
        region_name: Region to perform API operation (default: 'us-west-2')

    Returns:
        List of dictionaries containing stream details
    """
    # Required Paramaters (None)
    params: ListStreamsInput = {}

    # Optional Paramaters
    if exclusive_start_stream_name is not None:
        params['ExclusiveStartStreamName'] = exclusive_start_stream_name

    if limit is not None:
        params['Limit'] = limit

    if next_token is not None:
        params['NextToken'] = next_token

    # Call Kinesis API to list the streams
    kinesis = get_kinesis_client(region_name)
    response = kinesis.list_streams(**params)

    # Return List of Stream Details
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
    # Required Paramaters
    params: DescribeStreamSummaryInput = {}

    if stream_name is None and stream_arn is None:
        raise ValueError('Either stream_name or stream_arn must be provided')
    # Optional Parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
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
