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
from awslabs.kinesis_mcp_server.common import (
    CreateStreamInput,
    DescribeStreamSummaryInput,
    GetRecordsInput,
    ListStreamsInput,
    PutRecordsInput,
    handle_exceptions,
)
from loguru import logger
from mcp.server.fastmcp import FastMCP
from typing import Any, Dict, List


mcp = FastMCP(
    'awslabs.kinesis-mcp-server',
    instructions='Instructions for using this kinesis MCP server. This can be used by clients to improve the LLM'
    's understanding of available tools, resources, etc. It can be thought of like a '
    'hint'
    ' to the model. For example, this information MAY be added to the system prompt. Important to be clear, direct, and detailed.',
    dependencies=[
        'pydantic',
        'loguru',
    ],
    version='alpha',
)

kinesis = boto3.client('kinesis')


@mcp.tool('put_records')
@handle_exceptions
def put_records(
    stream_name: str, records: List[Dict[str, Any]], stream_arn: str = None
) -> Dict[str, Any]:
    """Writes multiple data records to a Kinesis data stream in a single call.

    Args:
        stream_name: The name of the stream to write to
        records: List of records to write to the stream
        stream_arn: ARN of the stream to write to

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
    response = kinesis.put_records(**params)

    # Return Sequence Number and Shard ID
    return response


@mcp.tool('get_records')
@handle_exceptions
def get_records(shard_iterator: str, limit: int = None, stream_arn: str = None) -> Dict[str, Any]:
    """Retrieves records from a Kinesis shard.

    Args:
        shard_iterator: The shard iterator to use for retrieving records
        limit: Maximum number of records to retrieve (default: None)
        stream_arn: ARN of the stream to retrieve records from

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
    response = kinesis.get_records(**params)

    # Return Records
    return response


@mcp.tool('create_stream')
@handle_exceptions
def create_stream(
    stream_name: str,
    shard_count: int = 1,
    stream_mode_details: Dict[str, str] = None,
    tags: Dict[str, str] = None,
) -> Dict[str, Any]:
    # TODO: There is a limit of 500 shards per region
    # TODO: Check for formatting of name possibly? Cannot have certain characters
    # TODO: Limit of 50 Tags - Test 200
    """Creates a new Kinesis data stream with the specified name and shard count.

    Args:
        stream_name: A name to identify the stream
        shard_count: Number of shards to create (default: 1)
        stream_mode_details: Details about the stream mode (default: {"StreamMode": "PROVISIONED"})
        tags: Tags to associate with the stream

    Returns:
        Dictionary containing the stream name and creation status
    """
    # Required Paramaters
    params: CreateStreamInput = {'StreamName': stream_name}

    # Optional Paramaters
    if stream_mode_details is None:
        stream_mode_details = {'StreamMode': 'PROVISIONED'}

    params['StreamModeDetails'] = stream_mode_details

    # Add ShardCount only for PROVISIONED mode
    if stream_mode_details.get('StreamMode') == 'PROVISIONED':
        params['ShardCount'] = shard_count

    if tags is not None:
        params['Tags'] = tags

    # Call Kinesis API to create the stream
    kinesis.create_stream(**params)

    # Return Status Details of the Stream Creation
    return {
        'StreamName': stream_name,
        'ShardCount': shard_count,
        'StreamModeDetails': stream_mode_details,
        'Tags': tags,
        'Status': 'CREATING',
    }


@mcp.tool('list_streams')
@handle_exceptions
def list_streams(
    limit: int = 100, exclusive_start_stream_name: str = None, next_token: str = None
) -> List[Dict[str, Any]]:
    """Lists the Kinesis data streams.

    Args:
        limit: Maximum number of streams to list (default: 100)
        exclusive_start_stream_name: Name of the stream to start listing from (default: None)
        next_token: Token for pagination (default: None)

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
    response = kinesis.list_streams(**params)

    # Return List of Stream Details
    return response


@mcp.tool('describe_stream_summary')
@handle_exceptions
def describe_stream_summary(stream_name: str, stream_arn: str) -> Dict[str, Any]:
    """Describes the stream summary.

    Args:
        stream_name: Name of the stream to describe
        stream_arn: ARN of the stream to describe

    Returns:
        Dictionary containing stream summary details
    """
    # Required Paramaters
    params: DescribeStreamSummaryInput = {}

    # Optional Parameters
    if stream_name is not None:
        params['StreamName'] = stream_name

    if stream_arn is not None:
        params['StreamARN'] = stream_arn

    # Call Kinesis API to describe the stream summary
    response = kinesis.describe_stream_summary(**params)

    # Return Stream Summary Details
    return response


def main():
    """Run the MCP server with CLI argument support."""
    logger.trace('A trace message.')
    logger.debug('A debug message.')
    logger.info('An info message.')
    logger.success('A success message.')
    logger.warning('A warning message.')
    logger.error('An error message.')
    logger.critical('A critical message.')

    mcp.run()


if __name__ == '__main__':
    main()
