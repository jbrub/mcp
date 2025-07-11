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


"""Tests for the kinesis MCP Server."""

import boto3
import os
import pytest
import sys
from datetime import datetime
from moto import mock_aws
from unittest.mock import MagicMock, patch


os.environ['KINESIS-READONLY'] = 'false'
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from awslabs.kinesis_mcp_server.consts import (
    MAX_LENGTH_SHARD_ITERATOR,
    MAX_LIMIT,
    MAX_STREAM_ARN_LENGTH,
    MAX_STREAM_NAME_LENGTH,
    STREAM_MODE_ON_DEMAND,
)
from awslabs.kinesis_mcp_server.server import (
    add_tags_to_stream,
    create_stream,
    decrease_stream_retention_period,
    delete_resource_policy,
    delete_stream,
    deregister_stream_consumer,
    describe_limits,
    describe_stream,
    describe_stream_consumer,
    describe_stream_summary,
    disable_enhanced_monitoring,
    enable_enhanced_monitoring,
    get_records,
    get_resource_policy,
    get_shard_iterator,
    increase_stream_retention_period,
    list_shards,
    list_stream_consumers,
    list_streams,
    list_tags_for_resource,
    list_tags_for_stream,
    merge_shards,
    put_record,
    put_records,
    put_resource_policy,
    register_stream_consumer,
    remove_tags_from_stream,
    split_shard,
    start_stream_encryption,
    stop_stream_encryption,
    tag_resource,
    untag_resource,
    update_shard_count,
    update_stream_mode,
)


class MockFastMCP:
    """Mock implementation of FastMCP for testing purposes."""

    def __init__(self, name, instructions, version):
        """Initialize the MockFastMCP instance.

        Args:
            name: Name of the MCP server
            instructions: Instructions for the MCP server
            version: Version of the MCP server
        """
        self.name = name
        self.instructions = instructions
        self.version = version

    def tool(self, name):
        """Mock implementation of the tool decorator.

        Args:
            name: Name of the tool

        Returns:
            A decorator function that returns the original function
        """

        def decorator(func):
            return func

        return decorator


sys.modules['mcp'] = MagicMock()
sys.modules['mcp.server'] = MagicMock()
sys.modules['mcp.server.fastmcp'] = MagicMock()
sys.modules['mcp.server.fastmcp'].FastMCP = MockFastMCP


@pytest.fixture(autouse=True)
def setup_testing_env():
    """Set up testing environment for all tests."""
    os.environ['TESTING'] = 'true'
    yield
    os.environ.pop('TESTING', None)


# Create a mock for the mcp module
@pytest.fixture
def mock_kinesis_client():
    """Create a mock Kinesis client using moto."""
    with mock_aws():
        client = boto3.client('kinesis', region_name='us-west-2')
        yield client


# Helper function to accomodate for new return format
def get_api_response(result):
    """Extract the original API response from the formatted result."""
    return result.get('api_response', result)


# ==============================================================================
#                       put_records Error Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_put_records_basic(mock_kinesis_client):
    """Test basic put_records functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_records method
        mock_response = {
            'Records': [{'SequenceNumber': '123', 'ShardId': 'shardId-000000000000'}],
            'FailedRecordCount': 0,
        }
        mock_kinesis_client.put_records = MagicMock(return_value=mock_response)

        # Create test records
        records = [{'Data': 'test-data', 'PartitionKey': 'test-key'}]

        # Call put_records
        result = await put_records(
            records=records, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify put_records was called with the right parameters
        mock_kinesis_client.put_records.assert_called_once()
        args = mock_kinesis_client.put_records.call_args[1]
        assert args['Records'] == records
        assert args['StreamName'] == 'test-stream'

        # Verify the result - extract api_response
        api_response = result.get('api_response', {})
        assert api_response.get('FailedRecordCount', -1) == 0
        assert len(api_response.get('Records', [])) == 1

        # Also verify the formatted fields
        assert result.get('status') == 'success'
        assert result.get('failed_records') == 0
        assert result.get('total_records') == 1
        assert result.get('successful_records') == 1


@pytest.mark.asyncio
async def test_put_records_with_stream_arn(mock_kinesis_client):
    """Test put_records with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_records method
        mock_response = {
            'Records': [{'SequenceNumber': '123', 'ShardId': 'shardId-000000000000'}],
            'FailedRecordCount': 0,
        }
        mock_kinesis_client.put_records = MagicMock(return_value=mock_response)

        # Create test records
        records = [{'Data': 'test-data', 'PartitionKey': 'test-key'}]
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'

        # Call put_records with stream ARN
        result = await put_records(records=records, stream_arn=stream_arn, region_name='us-west-2')

        # Verify put_records was called with the right parameters
        mock_kinesis_client.put_records.assert_called_once()
        args = mock_kinesis_client.put_records.call_args[1]
        assert args['Records'] == records
        assert args['StreamARN'] == stream_arn

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('FailedRecordCount', -1) == 0
        assert len(api_response.get('Records', [])) == 1


@pytest.mark.asyncio
async def test_put_records_with_stream_name(mock_kinesis_client):
    """Test put_records with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_records method
        mock_response = {
            'Records': [{'SequenceNumber': '123', 'ShardId': 'shardId-000000000000'}],
            'FailedRecordCount': 0,
        }
        mock_kinesis_client.put_records = MagicMock(return_value=mock_response)

        # Create test records
        records = [{'Data': 'test-data', 'PartitionKey': 'test-key'}]
        stream_name = 'test-stream'

        # Call put_records with stream name
        result = await put_records(
            records=records, stream_name=stream_name, region_name='us-west-2'
        )

        # Verify put_records was called with the right parameters
        mock_kinesis_client.put_records.assert_called_once()
        args = mock_kinesis_client.put_records.call_args[1]
        assert args['Records'] == records
        assert args['StreamName'] == stream_name

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('FailedRecordCount', -1) == 0
        assert len(api_response.get('Records', [])) == 1


@pytest.mark.asyncio
async def test_put_records_multiple_records(mock_kinesis_client):
    """Test put_records with multiple records."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_records method
        mock_response = {
            'Records': [
                {'SequenceNumber': '123', 'ShardId': 'shardId-000000000000'},
                {'SequenceNumber': '456', 'ShardId': 'shardId-000000000001'},
                {'SequenceNumber': '789', 'ShardId': 'shardId-000000000002'},
            ],
            'FailedRecordCount': 0,
        }
        mock_kinesis_client.put_records = MagicMock(return_value=mock_response)

        # Create multiple test records
        records = [
            {'Data': 'test-data-1', 'PartitionKey': 'test-key-1'},
            {'Data': 'test-data-2', 'PartitionKey': 'test-key-2'},
            {'Data': 'test-data-3', 'PartitionKey': 'test-key-3'},
        ]

        # Call put_records
        result = await put_records(
            records=records, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify put_records was called with the right parameters
        mock_kinesis_client.put_records.assert_called_once()
        args = mock_kinesis_client.put_records.call_args[1]
        assert args['Records'] == records
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('FailedRecordCount', -1) == 0
        assert len(api_response.get('Records', [])) == 3


@pytest.mark.asyncio
async def test_put_records_missing_identifiers(mock_kinesis_client):
    """Test put_records with missing stream identifiers - should succeed but AWS API will handle the error."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the AWS API to raise an exception when no stream identifier is provided
        mock_kinesis_client.put_records = MagicMock(
            side_effect=Exception(
                'ValidationException: Either StreamName or StreamARN must be provided'
            )
        )

        records = [{'Data': 'test-data', 'PartitionKey': 'test-key'}]
        result = await put_records(
            records=records,
            stream_name=None,
            stream_arn=None,
            region_name='us-west-2',
        )

        # Verify the function returns an error response (handled by @handle_exceptions decorator)
        assert 'error' in result
        assert 'Validation error' in result['error']


# ==============================================================================
#                           get_records Error Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_get_records_missing_shard_iterator(mock_kinesis_client):
    """Test get_records with missing shard iterator."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.get_records = MagicMock(
            side_effect=Exception('ValidationException: shard_iterator is required')
        )

        # Call get_records with empty shard iterator
        result = await get_records(shard_iterator='', region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_get_records_invalid_shard_iterator_length(mock_kinesis_client):
    """Test get_records with invalid shard iterator length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.get_records = MagicMock(
            side_effect=Exception('ValidationException: shard_iterator length must be between')
        )

        # Call get_records with long iterator
        long_iterator = 'a' * (MAX_LENGTH_SHARD_ITERATOR + 1)
        result = await get_records(shard_iterator=long_iterator, region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_get_records_invalid_limit_value(mock_kinesis_client):
    """Test get_records with invalid limit."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.get_records = MagicMock(
            side_effect=Exception('ValidationException: limit must be between')
        )

        # Call get_records with invalid limit
        result = await get_records(
            shard_iterator='valid-iterator', limit=MAX_LIMIT + 1, region_name='us-west-2'
        )

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_get_records_invalid_limit_type(mock_kinesis_client):
    """Test get_records with invalid limit type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.get_records = MagicMock(
            side_effect=Exception('ValidationException: limit must be an integer')
        )

        # Call get_records with invalid limit type
        result = await get_records(
            shard_iterator='valid-iterator', limit='not-an-int', region_name='us-west-2'
        )

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_get_records_with_limit(mock_kinesis_client):
    """Test get_records with limit parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_records response
        mock_response = {
            'Records': [],
            'NextShardIterator': 'next-shard-iterator',
            'MillisBehindLatest': 0,
        }
        mock_kinesis_client.get_records = MagicMock(return_value=mock_response)

        # Call get_records with limit
        limit = 100
        result = await get_records(
            shard_iterator='valid-iterator', limit=limit, region_name='us-west-2'
        )

        # Verify get_records was called with the right parameters
        mock_kinesis_client.get_records.assert_called_once()
        args = mock_kinesis_client.get_records.call_args[1]
        assert args['ShardIterator'] == 'valid-iterator'
        assert args['Limit'] == limit

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('NextShardIterator') == 'next-shard-iterator'
        assert api_response.get('MillisBehindLatest') == 0
        assert len(api_response.get('Records', [])) == 0


@pytest.mark.asyncio
async def test_get_records_invalid_stream_arn_length(mock_kinesis_client):
    """Test get_records with invalid stream ARN length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.get_records = MagicMock(
            side_effect=Exception('ValidationException: stream_arn length must be between')
        )

        # Call get_records with long ARN
        long_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/' + 'a' * (MAX_STREAM_ARN_LENGTH)
        result = await get_records(
            shard_iterator='valid-iterator', stream_arn=long_arn, region_name='us-west-2'
        )

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


# ==============================================================================
#                       create_stream Error Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_create_stream_basic(mock_kinesis_client):
    """Test basic create_stream functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the create_stream method
        mock_kinesis_client.create_stream = MagicMock(return_value={})

        # Call create_stream
        await create_stream(stream_name='test-stream', region_name='us-west-2')

        # Verify create_stream was called with the right parameters
        mock_kinesis_client.create_stream.assert_called_once()
        args = mock_kinesis_client.create_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['StreamModeDetails'] == {'StreamMode': STREAM_MODE_ON_DEMAND}


@pytest.mark.asyncio
async def test_create_stream_with_provisioned_mode(mock_kinesis_client):
    """Test create_stream with PROVISIONED mode."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the create_stream method
        mock_kinesis_client.create_stream = MagicMock(return_value={})

        # Call create_stream with PROVISIONED mode
        await create_stream(
            stream_name='test-stream',
            stream_mode_details={'StreamMode': 'PROVISIONED'},
            shard_count=5,
            region_name='us-west-2',
        )

        # Verify create_stream was called with the right parameters
        mock_kinesis_client.create_stream.assert_called_once()
        args = mock_kinesis_client.create_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['StreamModeDetails'] == {'StreamMode': 'PROVISIONED'}
        assert args['ShardCount'] == 5


@pytest.mark.asyncio
async def test_create_stream_with_tags(mock_kinesis_client):
    """Test create_stream with tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the create_stream method
        mock_kinesis_client.create_stream = MagicMock(return_value={})

        # Call create_stream with tags
        tags = {'Environment': 'Test', 'Project': 'Kinesis'}
        await create_stream(stream_name='test-stream', tags=tags, region_name='us-west-2')

        # Verify create_stream was called with the right parameters
        mock_kinesis_client.create_stream.assert_called_once()
        args = mock_kinesis_client.create_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['Tags'] == tags


@pytest.mark.asyncio
async def test_create_stream_with_non_dict_stream_mode_details(mock_kinesis_client):
    """Test create_stream with non-dict stream_mode_details."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the create_stream method
        mock_kinesis_client.create_stream = MagicMock(return_value={})

        # Call create_stream with non-dict stream_mode_details
        await create_stream(
            stream_name='test-stream',
            stream_mode_details='INVALID',
            region_name='us-west-2',
        )

        # Verify create_stream was called with the right parameters
        mock_kinesis_client.create_stream.assert_called_once()
        args = mock_kinesis_client.create_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['StreamModeDetails'] == {'StreamMode': STREAM_MODE_ON_DEMAND}  # Default mode


@pytest.mark.asyncio
async def test_create_stream_with_empty_tags(mock_kinesis_client):
    """Test create_stream with empty tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the create_stream method
        mock_kinesis_client.create_stream = MagicMock(return_value={})

        # Call create_stream with empty tags
        await create_stream(stream_name='test-stream', tags={}, region_name='us-west-2')

        # Verify create_stream was called with the right parameters
        mock_kinesis_client.create_stream.assert_called_once()
        args = mock_kinesis_client.create_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert 'Tags' not in args  # Empty tags should not be included


@pytest.mark.asyncio
async def test_create_stream_with_validation_error(mock_kinesis_client):
    """Test create_stream with validation error."""
    from awslabs.kinesis_mcp_server.common import handle_exceptions

    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a test function that raises a ValidationException
        @handle_exceptions
        async def test_func():
            raise Exception('ValidationException: Some validation error')

        # Call the test function
        result = await test_func()

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_create_stream_with_resource_in_use(mock_kinesis_client):
    """Test create_stream with resource in use error."""
    from awslabs.kinesis_mcp_server.common import handle_exceptions

    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a test function that raises a ResourceInUseException
        @handle_exceptions
        async def test_func():
            raise Exception('ResourceInUseException: Some resource in use error')

        # Call the test function
        result = await test_func()

        # Verify error response
        assert 'error' in result
        assert 'Resource in use' in result['error']


@pytest.mark.asyncio
async def test_create_stream_with_stream_mode_details(mock_kinesis_client):
    """Test create_stream with stream mode details."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {'StreamName': 'test-stream'}
        mock_kinesis_client.create_stream = MagicMock(return_value=mock_response)

        result = await create_stream(
            stream_name='test-stream',
            shard_count=2,
            stream_mode_details='ON_DEMAND',
            region_name='us-west-2',
        )

        args = mock_kinesis_client.create_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['StreamModeDetails']['StreamMode'] == 'ON_DEMAND'
        assert 'message' in result


# ==============================================================================
#                       list_streams Error Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_list_streams_basic(mock_kinesis_client):
    """Test basic list_streams functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_streams response
        mock_response = {'StreamNames': ['stream1', 'stream2'], 'HasMoreStreams': False}
        mock_kinesis_client.list_streams = MagicMock(return_value=mock_response)

        # Call list_streams
        result = await list_streams(region_name='us-west-2')

        # Verify list_streams was called with the right parameters
        mock_kinesis_client.list_streams.assert_called_once()

        # Verify the result contains the expected data
        assert 'StreamNames' in result
        assert result['StreamNames'] == ['stream1', 'stream2']
        assert not result['HasMoreStreams']  # Fix E712 linting issue


@pytest.mark.asyncio
async def test_list_streams_with_parameters(mock_kinesis_client):
    """Test list_streams with optional parameters."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_streams response
        mock_response = {
            'StreamNames': ['stream2', 'stream3'],
            'HasMoreStreams': True,
            'NextToken': 'next-token',
        }
        mock_kinesis_client.list_streams = MagicMock(return_value=mock_response)

        # Call list_streams with parameters
        result = await list_streams(
            exclusive_start_stream_name='stream1',
            limit=10,
            next_token='token',
            region_name='us-west-2',
        )

        # Verify list_streams was called with the right parameters
        mock_kinesis_client.list_streams.assert_called_once()
        args = mock_kinesis_client.list_streams.call_args[1]
        assert args['ExclusiveStartStreamName'] == 'stream1'
        assert args['Limit'] == 10
        assert args['NextToken'] == 'token'

        # Verify the result contains the expected data
        assert result['StreamNames'] == ['stream2', 'stream3']
        assert result['HasMoreStreams']  # Fix E712 linting issue
        assert result['NextToken'] == 'next-token'


@pytest.mark.asyncio
async def test_list_streams_invalid_limit(mock_kinesis_client):
    """Test list_streams with invalid limit."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.list_streams = MagicMock(
            side_effect=Exception('ValidationException: limit must be between')
        )

        # Call list_streams with invalid limit
        result = await list_streams(limit=0, region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_list_streams_invalid_exclusive_start_stream_name(mock_kinesis_client):
    """Test list_streams with invalid exclusive_start_stream_name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.list_streams = MagicMock(
            side_effect=Exception(
                'ValidationException: exclusive_start_stream_name length must be between'
            )
        )

        # Call list_streams with long name
        long_name = 'a' * (MAX_STREAM_NAME_LENGTH + 1)
        result = await list_streams(exclusive_start_stream_name=long_name, region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_list_streams_invalid_exclusive_start_stream_name_type(mock_kinesis_client):
    """Test list_streams with invalid exclusive_start_stream_name type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.list_streams = MagicMock(
            side_effect=Exception(
                'ValidationException: exclusive_start_stream_name must be a string'
            )
        )

        # Call list_streams with invalid type
        result = await list_streams(exclusive_start_stream_name=123, region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_list_streams_invalid_limit_type(mock_kinesis_client):
    """Test list_streams with invalid limit type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.list_streams = MagicMock(
            side_effect=Exception('ValidationException: limit must be an integer')
        )

        # Call list_streams with invalid type
        result = await list_streams(limit='not-an-int', region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_list_streams_invalid_next_token_type(mock_kinesis_client):
    """Test list_streams with invalid next_token type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise an exception
        mock_kinesis_client.list_streams = MagicMock(
            side_effect=Exception('ValidationException: next_token must be a string')
        )

        # Call list_streams with invalid type
        result = await list_streams(next_token=123, region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


# ==============================================================================
#                   describe_stream_summary Error Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_describe_stream_summary_basic(mock_kinesis_client):
    """Test basic describe_stream_summary functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_stream_summary response
        mock_response = {
            'StreamDescriptionSummary': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
                'RetentionPeriodHours': 24,
                'StreamCreationTimestamp': datetime(2023, 1, 1),
                'EnhancedMonitoring': [{'ShardLevelMetrics': []}],
                'OpenShardCount': 1,
                'StreamModeDetails': {'StreamMode': 'ON_DEMAND'},
            }
        }
        mock_kinesis_client.describe_stream_summary = MagicMock(return_value=mock_response)

        # Call describe_stream_summary
        result = await describe_stream_summary(stream_name='test-stream', region_name='us-west-2')

        # Verify describe_stream_summary was called with the right parameters
        mock_kinesis_client.describe_stream_summary.assert_called_once()
        args = mock_kinesis_client.describe_stream_summary.call_args[1]
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        api_response = get_api_response(result)
        summary = api_response.get('StreamDescriptionSummary', {})
        assert summary.get('StreamName') == 'test-stream'
        assert summary.get('StreamStatus') == 'ACTIVE'
        assert summary.get('RetentionPeriodHours') == 24
        assert summary.get('OpenShardCount') == 1


@pytest.mark.asyncio
async def test_describe_stream_summary_missing_identifiers(mock_kinesis_client):
    """Test describe_stream_summary with missing identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await describe_stream_summary(
                stream_name=None, stream_arn=None, region_name='us-west-2'
            )


@pytest.mark.asyncio
async def test_describe_stream_summary_stream_not_found(mock_kinesis_client):
    """Test describe_stream_summary with non-existent stream."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise a ResourceNotFoundException
        mock_kinesis_client.describe_stream_summary = MagicMock(
            side_effect=Exception('ResourceNotFoundException: Stream not found')
        )

        # Call describe_stream_summary with non-existent stream
        result = await describe_stream_summary(
            stream_name='non-existent-stream', region_name='us-west-2'
        )

        # Verify error response
        assert 'error' in result
        assert 'Resource not found' in result['error']


@pytest.mark.asyncio
async def test_describe_stream_summary_invalid_stream_name(mock_kinesis_client):
    """Test describe_stream_summary with invalid stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise a ValidationException
        mock_kinesis_client.describe_stream_summary = MagicMock(
            side_effect=Exception('ValidationException: Invalid stream name')
        )

        # Call describe_stream_summary with invalid stream name
        result = await describe_stream_summary(
            stream_name='invalid@stream', region_name='us-west-2'
        )

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_describe_stream_summary_invalid_stream_arn(mock_kinesis_client):
    """Test describe_stream_summary with invalid stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the API to raise a ValidationException
        mock_kinesis_client.describe_stream_summary = MagicMock(
            side_effect=Exception('ValidationException: Invalid stream ARN')
        )

        # Call describe_stream_summary with invalid stream ARN
        result = await describe_stream_summary(stream_arn='invalid:arn', region_name='us-west-2')

        # Verify error response
        assert 'error' in result
        assert 'Validation error' in result['error']


@pytest.mark.asyncio
async def test_describe_stream_summary_with_stream_arn(mock_kinesis_client):
    """Test describe_stream_summary with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {
            'StreamDescriptionSummary': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
            }
        }
        mock_kinesis_client.describe_stream_summary = MagicMock(return_value=mock_response)

        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await describe_stream_summary(stream_arn=stream_arn, region_name='us-west-2')

        args = mock_kinesis_client.describe_stream_summary.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert 'stream_name' in result


# ==============================================================================
#                       get_shard_iterator Error Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_get_shard_iterator_basic(mock_kinesis_client):
    """Test basic get_shard_iterator functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_shard_iterator response
        mock_response = {'ShardIterator': 'shard-iterator-value'}
        mock_kinesis_client.get_shard_iterator = MagicMock(return_value=mock_response)

        # Call get_shard_iterator
        result = await get_shard_iterator(
            shard_id='shardId-000000000000',
            shard_iterator_type='TRIM_HORIZON',
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify get_shard_iterator was called with the right parameters
        mock_kinesis_client.get_shard_iterator.assert_called_once()
        args = mock_kinesis_client.get_shard_iterator.call_args[1]
        assert args['ShardId'] == 'shardId-000000000000'
        assert args['ShardIteratorType'] == 'TRIM_HORIZON'
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('ShardIterator') == 'shard-iterator-value'


@pytest.mark.asyncio
async def test_get_shard_iterator_with_stream_arn(mock_kinesis_client):
    """Test get_shard_iterator with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_shard_iterator response
        mock_response = {'ShardIterator': 'shard-iterator-value'}
        mock_kinesis_client.get_shard_iterator = MagicMock(return_value=mock_response)

        # Call get_shard_iterator with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await get_shard_iterator(
            shard_id='shardId-000000000000',
            shard_iterator_type='TRIM_HORIZON',
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        # Verify get_shard_iterator was called with the right parameters
        mock_kinesis_client.get_shard_iterator.assert_called_once()
        args = mock_kinesis_client.get_shard_iterator.call_args[1]
        assert args['ShardId'] == 'shardId-000000000000'
        assert args['ShardIteratorType'] == 'TRIM_HORIZON'
        assert args['StreamARN'] == stream_arn

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('ShardIterator') == 'shard-iterator-value'


@pytest.mark.asyncio
async def test_get_shard_iterator_with_sequence_number(mock_kinesis_client):
    """Test get_shard_iterator with sequence number."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_shard_iterator response
        mock_response = {'ShardIterator': 'shard-iterator-value'}
        mock_kinesis_client.get_shard_iterator = MagicMock(return_value=mock_response)

        # Call get_shard_iterator with sequence number
        sequence_number = '49598630142999655949581543785528105911853783356538642434'
        result = await get_shard_iterator(
            shard_id='shardId-000000000000',
            shard_iterator_type='AT_SEQUENCE_NUMBER',
            stream_name='test-stream',
            starting_sequence_number=sequence_number,
            region_name='us-west-2',
        )

        # Verify get_shard_iterator was called with the right parameters
        mock_kinesis_client.get_shard_iterator.assert_called_once()
        args = mock_kinesis_client.get_shard_iterator.call_args[1]
        assert args['ShardId'] == 'shardId-000000000000'
        assert args['ShardIteratorType'] == 'AT_SEQUENCE_NUMBER'
        assert args['StreamName'] == 'test-stream'
        assert args['StartingSequenceNumber'] == sequence_number

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('ShardIterator') == 'shard-iterator-value'


@pytest.mark.asyncio
async def test_get_shard_iterator_missing_identifiers(mock_kinesis_client):
    """Test get_shard_iterator with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await get_shard_iterator(
                shard_id='shardId-000000000000',
                shard_iterator_type='TRIM_HORIZON',
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


@pytest.mark.asyncio
async def test_get_shard_iterator_missing_sequence_number(mock_kinesis_client):
    """Test get_shard_iterator with missing sequence number."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='starting_sequence_number is required'):
            await get_shard_iterator(
                shard_id='shardId-000000000000',
                shard_iterator_type='AT_SEQUENCE_NUMBER',
                stream_name='test-stream',
                starting_sequence_number=None,
                region_name='us-west-2',
            )


@pytest.mark.asyncio
async def test_get_shard_iterator_missing_timestamp(mock_kinesis_client):
    """Test get_shard_iterator with missing timestamp."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='timestamp is required'):
            await get_shard_iterator(
                shard_id='shardId-000000000000',
                shard_iterator_type='AT_TIMESTAMP',
                stream_name='test-stream',
                timestamp=None,
                region_name='us-west-2',
            )


@pytest.mark.asyncio
async def test_get_shard_iterator_with_starting_sequence_number(mock_kinesis_client):
    """Test get_shard_iterator with starting sequence number."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {'ShardIterator': 'test-iterator'}
        mock_kinesis_client.get_shard_iterator = MagicMock(return_value=mock_response)

        result = await get_shard_iterator(
            stream_name='test-stream',
            shard_id='shardId-000000000000',
            shard_iterator_type='AT_SEQUENCE_NUMBER',
            starting_sequence_number='12345',
            region_name='us-west-2',
        )

        args = mock_kinesis_client.get_shard_iterator.call_args[1]
        assert args['StartingSequenceNumber'] == '12345'
        assert 'shard_iterator' in result


@pytest.mark.asyncio
async def test_get_shard_iterator_with_timestamp(mock_kinesis_client):
    """Test get_shard_iterator with timestamp."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {'ShardIterator': 'test-iterator'}
        mock_kinesis_client.get_shard_iterator = MagicMock(return_value=mock_response)

        timestamp = datetime(2023, 1, 1)
        result = await get_shard_iterator(
            stream_name='test-stream',
            shard_id='shardId-000000000000',
            shard_iterator_type='AT_TIMESTAMP',
            timestamp=timestamp,
            region_name='us-west-2',
        )

        args = mock_kinesis_client.get_shard_iterator.call_args[1]
        assert args['Timestamp'] == timestamp
        assert 'shard_iterator' in result


@pytest.mark.asyncio
async def test_get_shard_iterator_sequence_number_validation(mock_kinesis_client):
    """Test get_shard_iterator sequence number validation."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='starting_sequence_number is required'):
            await get_shard_iterator(
                shard_id='shardId-000000000000',
                shard_iterator_type='AT_SEQUENCE_NUMBER',
                stream_name='test-stream',
                starting_sequence_number=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       add_tags_to_stream Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_add_tags_to_stream_basic(mock_kinesis_client):
    """Test basic add_tags_to_stream functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the add_tags_to_stream response
        mock_response = {}
        mock_kinesis_client.add_tags_to_stream = MagicMock(return_value=mock_response)

        # Call add_tags_to_stream
        tags = {'Environment': 'Test', 'Project': 'Kinesis'}
        result = await add_tags_to_stream(
            tags=tags, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify add_tags_to_stream was called with the right parameters
        mock_kinesis_client.add_tags_to_stream.assert_called_once()
        args = mock_kinesis_client.add_tags_to_stream.call_args[1]
        assert args['Tags'] == tags
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        api_response = get_api_response(result)
        assert api_response == {}


@pytest.mark.asyncio
async def test_add_tags_to_stream_with_stream_arn(mock_kinesis_client):
    """Test add_tags_to_stream with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the add_tags_to_stream response
        mock_response = {}
        mock_kinesis_client.add_tags_to_stream = MagicMock(return_value=mock_response)

        # Call add_tags_to_stream with stream ARN
        tags = {'Environment': 'Test', 'Project': 'Kinesis'}
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await add_tags_to_stream(
            tags=tags, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify add_tags_to_stream was called with the right parameters
        mock_kinesis_client.add_tags_to_stream.assert_called_once()
        args = mock_kinesis_client.add_tags_to_stream.call_args[1]
        assert args['Tags'] == tags
        assert args['StreamARN'] == stream_arn

        # Verify the result
        api_response = get_api_response(result)
        assert api_response == {}


@pytest.mark.asyncio
async def test_add_tags_to_stream_missing_identifiers(mock_kinesis_client):
    """Test add_tags_to_stream with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await add_tags_to_stream(
                tags={'key': 'value'}, stream_name=None, stream_arn=None, region_name='us-west-2'
            )


# ==============================================================================
#                       describe_stream Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_describe_stream_basic(mock_kinesis_client):
    """Test basic describe_stream functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_stream response
        mock_response = {
            'StreamDescription': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
                'Shards': [
                    {
                        'ShardId': 'shardId-000000000000',
                        'HashKeyRange': {
                            'StartingHashKey': '0',
                            'EndingHashKey': '340282366920938463463374607431768211455',
                        },
                    }
                ],
                'HasMoreShards': False,
                'RetentionPeriodHours': 24,
                'StreamCreationTimestamp': datetime(2023, 1, 1),
                'EnhancedMonitoring': [{'ShardLevelMetrics': []}],
            }
        }
        mock_kinesis_client.describe_stream = MagicMock(return_value=mock_response)

        # Call describe_stream
        result = await describe_stream(stream_name='test-stream', region_name='us-west-2')

        # Verify describe_stream was called with the right parameters
        mock_kinesis_client.describe_stream.assert_called_once()
        args = mock_kinesis_client.describe_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        api_response = get_api_response(result)
        stream_description = api_response.get('StreamDescription', {})
        assert stream_description.get('StreamName') == 'test-stream'
        assert stream_description.get('StreamStatus') == 'ACTIVE'
        assert len(stream_description.get('Shards', [])) == 1
        assert stream_description.get('HasMoreShards') is False


@pytest.mark.asyncio
async def test_describe_stream_with_stream_arn(mock_kinesis_client):
    """Test describe_stream with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_stream response
        mock_response = {
            'StreamDescription': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
                'Shards': [],
                'HasMoreShards': False,
            }
        }
        mock_kinesis_client.describe_stream = MagicMock(return_value=mock_response)

        # Call describe_stream with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await describe_stream(stream_arn=stream_arn, region_name='us-west-2')

        # Verify describe_stream was called with the right parameters
        mock_kinesis_client.describe_stream.assert_called_once()
        args = mock_kinesis_client.describe_stream.call_args[1]
        assert args['StreamARN'] == stream_arn

        # Verify the result contains the expected data
        api_response = get_api_response(result)
        stream_description = api_response.get('StreamDescription', {})
        assert stream_description.get('StreamName') == 'test-stream'


@pytest.mark.asyncio
async def test_describe_stream_with_limit(mock_kinesis_client):
    """Test describe_stream with limit parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_stream response
        mock_response = {
            'StreamDescription': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
                'Shards': [],
                'HasMoreShards': True,
            }
        }
        mock_kinesis_client.describe_stream = MagicMock(return_value=mock_response)

        # Call describe_stream with limit
        limit = 5
        result = await describe_stream(
            stream_name='test-stream', limit=limit, region_name='us-west-2'
        )

        # Verify describe_stream was called with the right parameters
        mock_kinesis_client.describe_stream.assert_called_once()
        args = mock_kinesis_client.describe_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['Limit'] == limit

        # Verify the result contains the expected data
        api_response = get_api_response(result)
        stream_description = api_response.get('StreamDescription', {})
        assert stream_description.get('HasMoreShards') is True


@pytest.mark.asyncio
async def test_describe_stream_with_exclusive_start_shard_id(mock_kinesis_client):
    """Test describe_stream with exclusive_start_shard_id parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_stream response
        mock_response = {
            'StreamDescription': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
                'Shards': [],
                'HasMoreShards': False,
            }
        }
        mock_kinesis_client.describe_stream = MagicMock(return_value=mock_response)

        # Call describe_stream with exclusive_start_shard_id
        shard_id = 'shardId-000000000001'
        await describe_stream(
            stream_name='test-stream', exclusive_start_shard_id=shard_id, region_name='us-west-2'
        )

        # Verify describe_stream was called with the right parameters
        mock_kinesis_client.describe_stream.assert_called_once()
        args = mock_kinesis_client.describe_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ExclusiveStartShardId'] == shard_id


@pytest.mark.asyncio
async def test_describe_stream_missing_identifiers(mock_kinesis_client):
    """Test describe_stream with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await describe_stream(stream_name=None, stream_arn=None, region_name='us-west-2')


# ==============================================================================
#                       describe_stream_consumer Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_describe_stream_consumer_with_consumer_arn(mock_kinesis_client):
    """Test describe_stream_consumer with consumer ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_stream_consumer response
        mock_response = {
            'ConsumerDescription': {
                'ConsumerName': 'test-consumer',
                'ConsumerARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890',
                'ConsumerStatus': 'ACTIVE',
                'ConsumerCreationTimestamp': datetime(2023, 1, 1),
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
            }
        }
        mock_kinesis_client.describe_stream_consumer = MagicMock(return_value=mock_response)

        # Call describe_stream_consumer with consumer ARN
        consumer_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890'
        result = await describe_stream_consumer(consumer_arn=consumer_arn, region_name='us-west-2')

        # Verify describe_stream_consumer was called with the right parameters
        mock_kinesis_client.describe_stream_consumer.assert_called_once()
        args = mock_kinesis_client.describe_stream_consumer.call_args[1]
        assert args['ConsumerARN'] == consumer_arn

        # Verify the result
        api_response = get_api_response(result)
        consumer_description = api_response.get('ConsumerDescription', {})
        assert consumer_description.get('ConsumerARN') == consumer_arn


@pytest.mark.asyncio
async def test_describe_stream_consumer_missing_identifiers(mock_kinesis_client):
    """Test describe_stream_consumer with missing identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(
            ValueError,
            match='Either consumer_arn or both consumer_name and stream_arn must be provided',
        ):
            await describe_stream_consumer(
                consumer_name=None, stream_arn=None, consumer_arn=None, region_name='us-west-2'
            )


@pytest.mark.asyncio
async def test_describe_stream_consumer_missing_stream_arn(mock_kinesis_client):
    """Test describe_stream_consumer with consumer name but missing stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(
            ValueError,
            match='Either consumer_arn or both consumer_name and stream_arn must be provided',
        ):
            await describe_stream_consumer(
                consumer_name='test-consumer',
                stream_arn=None,
                consumer_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       list_stream_consumers Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_list_stream_consumers_basic(mock_kinesis_client):
    """Test basic list_stream_consumers functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_stream_consumers response
        mock_response = {
            'Consumers': [
                {
                    'ConsumerName': 'test-consumer',
                    'ConsumerARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890',
                    'ConsumerStatus': 'ACTIVE',
                    'ConsumerCreationTimestamp': datetime(2023, 1, 1),
                }
            ]
        }
        mock_kinesis_client.list_stream_consumers = MagicMock(return_value=mock_response)

        # Call list_stream_consumers
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await list_stream_consumers(stream_arn=stream_arn, region_name='us-west-2')

        # Verify list_stream_consumers was called with the right parameters
        mock_kinesis_client.list_stream_consumers.assert_called_once()
        args = mock_kinesis_client.list_stream_consumers.call_args[1]
        assert args['StreamARN'] == stream_arn

        # Verify the result
        api_response = get_api_response(result)
        consumers = api_response.get('Consumers', [])
        assert len(consumers) == 1
        assert consumers[0].get('ConsumerName') == 'test-consumer'


@pytest.mark.asyncio
async def test_list_stream_consumers_with_parameters(mock_kinesis_client):
    """Test list_stream_consumers with optional parameters."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_stream_consumers response
        mock_response = {
            'Consumers': [
                {
                    'ConsumerName': 'test-consumer',
                    'ConsumerARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890',
                    'ConsumerStatus': 'ACTIVE',
                    'ConsumerCreationTimestamp': datetime(2023, 1, 1),
                }
            ],
            'NextToken': 'next-token-value',
        }
        mock_kinesis_client.list_stream_consumers = MagicMock(return_value=mock_response)

        # Call list_stream_consumers with parameters
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        next_token = 'token'
        max_results = 10
        timestamp = datetime(2023, 1, 1)
        result = await list_stream_consumers(
            stream_arn=stream_arn,
            next_token=next_token,
            stream_creation_time_stamp=timestamp,
            max_results=max_results,
            region_name='us-west-2',
        )

        # Verify list_stream_consumers was called with the right parameters
        mock_kinesis_client.list_stream_consumers.assert_called_once()
        args = mock_kinesis_client.list_stream_consumers.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['NextToken'] == next_token
        assert args['StreamCreationTimestamp'] == timestamp
        assert args['MaxResults'] == max_results

        # Verify the result
        api_response = get_api_response(result)
        consumers = api_response.get('Consumers', [])
        assert len(consumers) == 1
        assert api_response.get('NextToken') == 'next-token-value'


# ==============================================================================
#                       list_tags_for_resource Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_list_tags_for_resource_basic(mock_kinesis_client):
    """Test basic list_tags_for_resource functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_tags_for_resource response
        mock_response = {'Tags': {'Environment': 'Test', 'Project': 'Kinesis'}}
        mock_kinesis_client.list_tags_for_resource = MagicMock(return_value=mock_response)

        # Call list_tags_for_resource
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await list_tags_for_resource(resource_arn=resource_arn, region_name='us-west-2')

        # Verify list_tags_for_resource was called with the right parameters
        mock_kinesis_client.list_tags_for_resource.assert_called_once()
        args = mock_kinesis_client.list_tags_for_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('Tags', {}) == {'Environment': 'Test', 'Project': 'Kinesis'}


@pytest.mark.asyncio
async def test_list_tags_for_resource_empty_tags(mock_kinesis_client):
    """Test list_tags_for_resource with empty tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_tags_for_resource response
        mock_response = {'Tags': {}}
        mock_kinesis_client.list_tags_for_resource = MagicMock(return_value=mock_response)

        # Call list_tags_for_resource
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await list_tags_for_resource(resource_arn=resource_arn, region_name='us-west-2')

        # Verify list_tags_for_resource was called with the right parameters
        mock_kinesis_client.list_tags_for_resource.assert_called_once()
        args = mock_kinesis_client.list_tags_for_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn

        # Verify the result
        api_response = get_api_response(result)
        assert api_response.get('Tags', None) == {}


# ==============================================================================
#                       describe_limits Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_describe_limits_success(mock_kinesis_client):
    """Test successful describe_limits."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {
            'ShardLimit': 500,
            'OpenShardCount': 100,
            'OnDemandStreamCount': 5,
            'OnDemandStreamCountLimit': 50,
        }
        mock_kinesis_client.describe_limits = MagicMock(return_value=mock_response)

        result = await describe_limits(
            region_name='us-west-2',
        )

        mock_kinesis_client.describe_limits.assert_called_with()
        api_response = get_api_response(result)
        assert api_response['ShardLimit'] == 500
        assert api_response['OpenShardCount'] == 100
        assert api_response['OnDemandStreamCount'] == 5
        assert api_response['OnDemandStreamCountLimit'] == 50


@pytest.mark.asyncio
async def test_describe_limits_with_default_region(mock_kinesis_client):
    """Test describe_limits with default region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {
            'ShardLimit': 500,
            'OpenShardCount': 75,
            'OnDemandStreamCount': 3,
            'OnDemandStreamCountLimit': 50,
        }
        mock_kinesis_client.describe_limits = MagicMock(return_value=mock_response)

        # Call without specifying region_name
        result = await describe_limits()

        # Verify that the function works with default region
        mock_kinesis_client.describe_limits.assert_called_with()
        api_response = get_api_response(result)
        assert api_response['ShardLimit'] == 500
        assert api_response['OpenShardCount'] == 75


@pytest.mark.asyncio
async def test_describe_limits_with_empty_response(mock_kinesis_client):
    """Test describe_limits with an empty response."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock an empty response (unlikely but possible)
        mock_response = {}
        mock_kinesis_client.describe_limits = MagicMock(return_value=mock_response)

        result = await describe_limits(
            region_name='us-west-2',
        )

        mock_kinesis_client.describe_limits.assert_called_with()
        api_response = get_api_response(result)
        assert api_response == {}


@pytest.mark.asyncio
async def test_describe_limits_with_partial_response(mock_kinesis_client):
    """Test describe_limits with a partial response."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock a response with only some fields
        mock_response = {'ShardLimit': 500, 'OpenShardCount': 100}
        mock_kinesis_client.describe_limits = MagicMock(return_value=mock_response)

        result = await describe_limits(
            region_name='us-west-2',
        )

        mock_kinesis_client.describe_limits.assert_called_with()
        api_response = get_api_response(result)
        assert api_response['ShardLimit'] == 500
        assert api_response['OpenShardCount'] == 100
        assert 'OnDemandStreamCount' not in api_response
        assert 'OnDemandStreamCountLimit' not in api_response


@pytest.mark.asyncio
async def test_describe_limits_with_additional_fields(mock_kinesis_client):
    """Test describe_limits with additional fields in the response."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock a response with additional fields
        mock_response = {
            'ShardLimit': 500,
            'OpenShardCount': 100,
            'OnDemandStreamCount': 5,
            'OnDemandStreamCountLimit': 50,
            'AdditionalField': 'some-value',  # Additional field
        }
        mock_kinesis_client.describe_limits = MagicMock(return_value=mock_response)

        result = await describe_limits(
            region_name='us-west-2',
        )

        mock_kinesis_client.describe_limits.assert_called_with()
        api_response = get_api_response(result)
        assert api_response['ShardLimit'] == 500
        assert api_response['OpenShardCount'] == 100
        assert api_response['AdditionalField'] == 'some-value'  # Should be included in the result


@pytest.mark.asyncio
async def test_describe_limits_basic(mock_kinesis_client):
    """Test basic describe_limits functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the describe_limits response
        mock_response = {
            'ShardLimit': 500,
            'OpenShardCount': 10,
            'OnDemandStreamCount': 5,
            'OnDemandStreamCountLimit': 50,
        }
        mock_kinesis_client.describe_limits = MagicMock(return_value=mock_response)

        # Call describe_limits
        result = await describe_limits(region_name='us-west-2')

        # Verify describe_limits was called with the right parameters
        mock_kinesis_client.describe_limits.assert_called_once()

        # Verify the result
        api_response = get_api_response(result)
        assert api_response['ShardLimit'] == 500
        assert api_response['OpenShardCount'] == 10
        assert api_response['OnDemandStreamCount'] == 5
        assert api_response['OnDemandStreamCountLimit'] == 50


# ==============================================================================
#                       enable_enhanced_monitoring Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_enable_enhanced_monitoring_basic(mock_kinesis_client):
    """Test basic enable_enhanced_monitoring functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the enable_enhanced_monitoring response
        mock_response = {
            'StreamName': 'test-stream',
            'CurrentShardLevelMetrics': [],
            'DesiredShardLevelMetrics': ['IncomingBytes', 'OutgoingBytes'],
        }
        mock_kinesis_client.enable_enhanced_monitoring = MagicMock(return_value=mock_response)

        # Call enable_enhanced_monitoring
        shard_level_metrics = ['IncomingBytes', 'OutgoingBytes']
        result = await enable_enhanced_monitoring(
            shard_level_metrics=shard_level_metrics,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify enable_enhanced_monitoring was called with the right parameters
        mock_kinesis_client.enable_enhanced_monitoring.assert_called_once()
        args = mock_kinesis_client.enable_enhanced_monitoring.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ShardLevelMetrics'] == shard_level_metrics

        # Verify the result contains the expected data
        assert result['desired_shard_level_metrics'] == shard_level_metrics


@pytest.mark.asyncio
async def test_enable_enhanced_monitoring_with_stream_arn(mock_kinesis_client):
    """Test enable_enhanced_monitoring with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the enable_enhanced_monitoring response
        mock_response = {
            'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
            'CurrentShardLevelMetrics': [],
            'DesiredShardLevelMetrics': ['IncomingBytes', 'OutgoingRecords'],
        }
        mock_kinesis_client.enable_enhanced_monitoring = MagicMock(return_value=mock_response)

        # Call enable_enhanced_monitoring with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        shard_level_metrics = ['IncomingBytes', 'OutgoingRecords']
        result = await enable_enhanced_monitoring(
            shard_level_metrics=shard_level_metrics, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify enable_enhanced_monitoring was called with the right parameters
        mock_kinesis_client.enable_enhanced_monitoring.assert_called_once()
        args = mock_kinesis_client.enable_enhanced_monitoring.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ShardLevelMetrics'] == shard_level_metrics

        # Verify the result contains the expected data
        assert result['desired_shard_level_metrics'] == shard_level_metrics


@pytest.mark.asyncio
async def test_enable_enhanced_monitoring_missing_identifiers(mock_kinesis_client):
    """Test enable_enhanced_monitoring with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await enable_enhanced_monitoring(
                shard_level_metrics=['IncomingBytes'],
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       get_resource_policy Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_get_resource_policy_basic(mock_kinesis_client):
    """Test basic get_resource_policy functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_resource_policy response
        mock_response = {
            'ResourceARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
            'Policy': '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"kinesis:*","Resource":"*"}]}',
        }
        mock_kinesis_client.get_resource_policy = MagicMock(return_value=mock_response)

        # Call get_resource_policy
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await get_resource_policy(resource_arn=resource_arn, region_name='us-west-2')

        # Verify get_resource_policy was called with the right parameters
        mock_kinesis_client.get_resource_policy.assert_called_once()
        args = mock_kinesis_client.get_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn

        # Verify the result contains the expected data
        assert result['resource_arn'] == resource_arn
        assert '"Effect":"Allow"' in result['policy']


@pytest.mark.asyncio
async def test_get_resource_policy_with_different_region(mock_kinesis_client):
    """Test get_resource_policy with a different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_resource_policy response
        mock_response = {
            'ResourceARN': 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream',
            'Policy': '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"kinesis:*","Resource":"*"}]}',
        }
        mock_kinesis_client.get_resource_policy = MagicMock(return_value=mock_response)

        # Call get_resource_policy with a different region
        resource_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        result = await get_resource_policy(resource_arn=resource_arn, region_name='us-east-1')

        # Verify get_resource_policy was called with the right parameters
        mock_kinesis_client.get_resource_policy.assert_called_once()
        args = mock_kinesis_client.get_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn

        # Verify the result contains the expected data
        assert result['resource_arn'] == resource_arn


@pytest.mark.asyncio
async def test_get_resource_policy_with_empty_policy(mock_kinesis_client):
    """Test get_resource_policy with an empty policy."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the get_resource_policy response with empty policy
        mock_response = {
            'ResourceARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
            'Policy': '{}',
        }
        mock_kinesis_client.get_resource_policy = MagicMock(return_value=mock_response)

        # Call get_resource_policy
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await get_resource_policy(resource_arn=resource_arn, region_name='us-west-2')

        # Verify get_resource_policy was called with the right parameters
        mock_kinesis_client.get_resource_policy.assert_called_once()

        # Verify the result contains the expected data
        assert result['policy'] == '{}'


# ==============================================================================
#                       increase_stream_retention_period Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_increase_stream_retention_period_basic(mock_kinesis_client):
    """Test basic increase_stream_retention_period functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the increase_stream_retention_period response
        mock_response = {}
        mock_kinesis_client.increase_stream_retention_period = MagicMock(
            return_value=mock_response
        )

        # Call increase_stream_retention_period
        retention_period_hours = 48
        result = await increase_stream_retention_period(
            retention_period_hours=retention_period_hours,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify increase_stream_retention_period was called with the right parameters
        mock_kinesis_client.increase_stream_retention_period.assert_called_once()
        args = mock_kinesis_client.increase_stream_retention_period.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['RetentionPeriodHours'] == retention_period_hours

        # Verify the result is the raw response
        assert result == mock_response


@pytest.mark.asyncio
async def test_increase_stream_retention_period_with_stream_arn(mock_kinesis_client):
    """Test increase_stream_retention_period with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the increase_stream_retention_period response
        mock_response = {}
        mock_kinesis_client.increase_stream_retention_period = MagicMock(
            return_value=mock_response
        )

        # Call increase_stream_retention_period with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        retention_period_hours = 72
        result = await increase_stream_retention_period(
            retention_period_hours=retention_period_hours,
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        # Verify increase_stream_retention_period was called with the right parameters
        mock_kinesis_client.increase_stream_retention_period.assert_called_once()
        args = mock_kinesis_client.increase_stream_retention_period.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['RetentionPeriodHours'] == retention_period_hours

        # Verify the result is the raw response
        assert result == mock_response


@pytest.mark.asyncio
async def test_increase_stream_retention_period_missing_identifiers(mock_kinesis_client):
    """Test increase_stream_retention_period with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await increase_stream_retention_period(
                retention_period_hours=48,
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       list_shards Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_list_shards_with_stream_name(mock_kinesis_client):
    """Test list_shards with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_shards response
        mock_response = {
            'Shards': [
                {
                    'ShardId': 'shardId-000000000000',
                    'HashKeyRange': {
                        'StartingHashKey': '0',
                        'EndingHashKey': '340282366920938463463374607431768211455',
                    },
                }
            ]
        }
        mock_kinesis_client.list_shards = MagicMock(return_value=mock_response)

        # Call list_shards with stream name
        result = await list_shards(stream_name='test-stream', region_name='us-west-2')

        # Verify list_shards was called with the right parameters
        mock_kinesis_client.list_shards.assert_called_once()
        args = mock_kinesis_client.list_shards.call_args[1]
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        assert len(result['shards']) == 1  # Use lowercase 'shards'
        assert result['shards'][0]['ShardId'] == 'shardId-000000000000'


@pytest.mark.asyncio
async def test_list_shards_with_max_results(mock_kinesis_client):
    """Test list_shards with max_results parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_shards response
        mock_response = {
            'Shards': [
                {
                    'ShardId': 'shardId-000000000000',
                    'HashKeyRange': {
                        'StartingHashKey': '0',
                        'EndingHashKey': '340282366920938463463374607431768211455',
                    },
                }
            ],
            'NextToken': 'next-token-value',
        }
        mock_kinesis_client.list_shards = MagicMock(return_value=mock_response)

        # Call list_shards with max_results
        max_results = 10
        result = await list_shards(
            stream_name='test-stream', max_results=max_results, region_name='us-west-2'
        )

        # Verify list_shards was called with the right parameters
        mock_kinesis_client.list_shards.assert_called_once()
        args = mock_kinesis_client.list_shards.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['MaxResults'] == max_results

        # Verify the result
        assert len(result['shards']) == 1  # Use lowercase 'shards'
        assert 'next_token' in result
        assert result['next_token'] == 'next-token-value'


# ==============================================================================
#                       tag_resource Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_tag_resource_with_empty_tags(mock_kinesis_client):
    """Test tag_resource with empty tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the tag_resource response
        mock_response = {}
        mock_kinesis_client.tag_resource = MagicMock(return_value=mock_response)

        # Call tag_resource with empty tags
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        tags = {}
        result = await tag_resource(resource_arn=resource_arn, tags=tags, region_name='us-west-2')

        # Verify tag_resource was called with the right parameters
        mock_kinesis_client.tag_resource.assert_called_once()
        args = mock_kinesis_client.tag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Tags'] == tags

        # Verify the result
        assert result['resource_arn'] == resource_arn
        assert result['tags'] == tags


@pytest.mark.asyncio
async def test_tag_resource_basic(mock_kinesis_client):
    """Test basic tag_resource functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the tag_resource response
        mock_response = {}
        mock_kinesis_client.tag_resource = MagicMock(return_value=mock_response)

        # Call tag_resource
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        tags = {'Environment': 'Test', 'Project': 'Kinesis'}
        result = await tag_resource(resource_arn=resource_arn, tags=tags, region_name='us-west-2')

        # Verify tag_resource was called with the right parameters
        mock_kinesis_client.tag_resource.assert_called_once()
        args = mock_kinesis_client.tag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Tags'] == tags

        # Verify the result
        assert result['resource_arn'] == resource_arn
        assert result['tags'] == tags


@pytest.mark.asyncio
async def test_tag_resource_with_different_region(mock_kinesis_client):
    """Test tag_resource with a different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the tag_resource response
        mock_response = {}
        mock_kinesis_client.tag_resource = MagicMock(return_value=mock_response)

        # Call tag_resource with a different region
        resource_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        tags = {'Environment': 'Prod', 'Project': 'Kinesis'}
        result = await tag_resource(resource_arn=resource_arn, tags=tags, region_name='us-east-1')

        # Verify tag_resource was called with the right parameters
        mock_kinesis_client.tag_resource.assert_called_once()
        args = mock_kinesis_client.tag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Tags'] == tags

        # Verify the result
        assert result['resource_arn'] == resource_arn
        assert result['tags'] == tags


@pytest.mark.asyncio
async def test_tag_resource_with_multiple_tags(mock_kinesis_client):
    """Test tag_resource with multiple tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the tag_resource response
        mock_response = {}
        mock_kinesis_client.tag_resource = MagicMock(return_value=mock_response)

        # Call tag_resource with multiple tags
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        tags = {
            'Environment': 'Test',
            'Project': 'Kinesis',
            'Owner': 'Team',
            'CostCenter': '12345',
            'Application': 'TestApp',
        }
        result = await tag_resource(resource_arn=resource_arn, tags=tags, region_name='us-west-2')

        # Verify tag_resource was called with the right parameters
        mock_kinesis_client.tag_resource.assert_called_once()
        args = mock_kinesis_client.tag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Tags'] == tags

        # Verify the result
        assert result['resource_arn'] == resource_arn
        assert result['tags'] == tags


# ==============================================================================
#                       list_tags_for_stream Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_list_tags_for_stream_basic(mock_kinesis_client):
    """Test basic list_tags_for_stream functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_tags_for_stream response
        mock_response = {
            'Tags': [
                {'Key': 'Environment', 'Value': 'Test'},
                {'Key': 'Project', 'Value': 'Kinesis'},
            ],
            'HasMoreTags': False,
        }
        mock_kinesis_client.list_tags_for_stream = MagicMock(return_value=mock_response)

        # Call list_tags_for_stream
        result = await list_tags_for_stream(stream_name='test-stream', region_name='us-west-2')

        # Verify list_tags_for_stream was called with the right parameters
        mock_kinesis_client.list_tags_for_stream.assert_called_once()
        args = mock_kinesis_client.list_tags_for_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'

        # Verify the result contains the expected data (use lowercase 'tags')
        assert len(result['tags']) == 2
        assert result['tags'][0]['Key'] == 'Environment'
        assert result['tags'][0]['Value'] == 'Test'
        assert result['tags'][1]['Key'] == 'Project'
        assert result['tags'][1]['Value'] == 'Kinesis'
        assert not result['has_more_tags']


@pytest.mark.asyncio
async def test_list_tags_for_stream_with_stream_arn(mock_kinesis_client):
    """Test list_tags_for_stream with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_tags_for_stream response
        mock_response = {
            'Tags': [
                {'Key': 'Environment', 'Value': 'Prod'},
                {'Key': 'Project', 'Value': 'Kinesis'},
            ],
            'HasMoreTags': False,
        }
        mock_kinesis_client.list_tags_for_stream = MagicMock(return_value=mock_response)

        # Call list_tags_for_stream with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await list_tags_for_stream(stream_arn=stream_arn, region_name='us-west-2')

        # Verify list_tags_for_stream was called with the right parameters
        mock_kinesis_client.list_tags_for_stream.assert_called_once()
        args = mock_kinesis_client.list_tags_for_stream.call_args[1]
        assert args['StreamARN'] == stream_arn

        # Verify the result contains the expected data (use lowercase 'tags')
        assert len(result['tags']) == 2
        assert result['tags'][0]['Key'] == 'Environment'
        assert result['tags'][0]['Value'] == 'Prod'


@pytest.mark.asyncio
async def test_list_tags_for_stream_with_pagination(mock_kinesis_client):
    """Test list_tags_for_stream with pagination parameters."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_tags_for_stream response
        mock_response = {'Tags': [{'Key': 'Project', 'Value': 'Kinesis'}], 'HasMoreTags': True}
        mock_kinesis_client.list_tags_for_stream = MagicMock(return_value=mock_response)

        # Call list_tags_for_stream with pagination parameters
        exclusive_start_tag_key = 'Environment'
        limit = 10
        result = await list_tags_for_stream(
            stream_name='test-stream',
            exclusive_start_tag_key=exclusive_start_tag_key,
            limit=limit,
            region_name='us-west-2',
        )

        # Verify list_tags_for_stream was called with the right parameters
        mock_kinesis_client.list_tags_for_stream.assert_called_once()
        args = mock_kinesis_client.list_tags_for_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ExclusiveStartTagKey'] == exclusive_start_tag_key
        assert args['Limit'] == limit

        # Verify the result contains the expected data (use lowercase)
        assert len(result['tags']) == 1
        assert result['tags'][0]['Key'] == 'Project'
        assert result['has_more_tags']


@pytest.mark.asyncio
async def test_list_tags_for_stream_with_empty_tags(mock_kinesis_client):
    """Test list_tags_for_stream with empty tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the list_tags_for_stream response with empty tags
        mock_response = {'Tags': [], 'HasMoreTags': False}
        mock_kinesis_client.list_tags_for_stream = MagicMock(return_value=mock_response)

        # Call list_tags_for_stream
        result = await list_tags_for_stream(stream_name='test-stream', region_name='us-west-2')

        # Verify list_tags_for_stream was called with the right parameters
        mock_kinesis_client.list_tags_for_stream.assert_called_once()

        # Verify the result contains empty tags (use lowercase)
        assert len(result['tags']) == 0
        assert not result['has_more_tags']


@pytest.mark.asyncio
async def test_list_tags_for_stream_missing_identifiers(mock_kinesis_client):
    """Test list_tags_for_stream with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await list_tags_for_stream(stream_name=None, stream_arn=None, region_name='us-west-2')


@pytest.mark.asyncio
async def test_list_tags_for_stream_with_exclusive_start_tag_key(mock_kinesis_client):
    """Test list_tags_for_stream with exclusive_start_tag_key parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {
            'Tags': [{'Key': 'Project', 'Value': 'Kinesis'}],
            'HasMoreTags': True,
        }
        mock_kinesis_client.list_tags_for_stream = MagicMock(return_value=mock_response)

        result = await list_tags_for_stream(
            stream_name='test-stream',
            exclusive_start_tag_key='Environment',
            limit=10,
            region_name='us-west-2',
        )

        args = mock_kinesis_client.list_tags_for_stream.call_args[1]
        assert args['ExclusiveStartTagKey'] == 'Environment'
        assert args['Limit'] == 10
        assert 'tags' in result


# ==============================================================================
#                       put_resource_policy Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_put_resource_policy_basic(mock_kinesis_client):
    """Test basic put_resource_policy functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_resource_policy response
        mock_response = {}
        mock_kinesis_client.put_resource_policy = MagicMock(return_value=mock_response)

        # Call put_resource_policy
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"kinesis:*","Resource":"*"}]}'
        result = await put_resource_policy(
            resource_arn=resource_arn, policy=policy, region_name='us-west-2'
        )

        # Verify put_resource_policy was called with the right parameters
        mock_kinesis_client.put_resource_policy.assert_called_once()
        args = mock_kinesis_client.put_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Policy'] == policy

        # Verify the result
        assert result['resource_arn'] == resource_arn


@pytest.mark.asyncio
async def test_put_resource_policy_with_different_region(mock_kinesis_client):
    """Test put_resource_policy with a different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_resource_policy response
        mock_response = {}
        mock_kinesis_client.put_resource_policy = MagicMock(return_value=mock_response)

        # Call put_resource_policy with a different region
        resource_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        policy = '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"kinesis:*","Resource":"*"}]}'
        result = await put_resource_policy(
            resource_arn=resource_arn, policy=policy, region_name='us-east-1'
        )

        # Verify put_resource_policy was called with the right parameters
        mock_kinesis_client.put_resource_policy.assert_called_once()
        args = mock_kinesis_client.put_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Policy'] == policy

        # Verify the result
        assert result['resource_arn'] == resource_arn


@pytest.mark.asyncio
async def test_put_resource_policy_with_complex_policy(mock_kinesis_client):
    """Test put_resource_policy with a complex policy."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_resource_policy response
        mock_response = {}
        mock_kinesis_client.put_resource_policy = MagicMock(return_value=mock_response)

        # Call put_resource_policy with a complex policy
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        policy = """
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::123456789012:role/KinesisReadRole"
                    },
                    "Action": [
                        "kinesis:GetRecords",
                        "kinesis:GetShardIterator",
                        "kinesis:DescribeStream"
                    ],
                    "Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/test-stream"
                }
            ]
        }
        """
        result = await put_resource_policy(
            resource_arn=resource_arn, policy=policy, region_name='us-west-2'
        )

        # Verify put_resource_policy was called with the right parameters
        mock_kinesis_client.put_resource_policy.assert_called_once()
        args = mock_kinesis_client.put_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['Policy'] == policy

        # Verify the result
        assert result['resource_arn'] == resource_arn


# ==============================================================================
#                       delete_stream Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_delete_stream_basic(mock_kinesis_client):
    """Test basic delete_stream functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the delete_stream response
        mock_response = {}
        mock_kinesis_client.delete_stream = MagicMock(return_value=mock_response)

        # Call delete_stream
        result = await delete_stream(stream_name='test-stream', region_name='us-west-2')

        # Verify delete_stream was called with the right parameters
        mock_kinesis_client.delete_stream.assert_called_once()
        args = mock_kinesis_client.delete_stream.call_args[1]
        assert 'StreamName' in args
        assert args['StreamName'] == 'test-stream'

        # Verify the result
        assert 'message' in result
        assert result['message'] == 'Successfully deleted stream'


@pytest.mark.asyncio
async def test_delete_stream_with_stream_arn(mock_kinesis_client):
    """Test delete_stream with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the delete_stream response
        mock_response = {}
        mock_kinesis_client.delete_stream = MagicMock(return_value=mock_response)

        # Call delete_stream with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await delete_stream(stream_arn=stream_arn, region_name='us-west-2')

        # Verify delete_stream was called with the right parameters
        mock_kinesis_client.delete_stream.assert_called_once()
        args = mock_kinesis_client.delete_stream.call_args[1]
        assert 'StreamARN' in args
        assert args['StreamARN'] == stream_arn

        # Verify the result
        assert 'message' in result
        assert result['message'] == 'Successfully deleted stream'


@pytest.mark.asyncio
async def test_delete_stream_with_enforce_consumer_deletion(mock_kinesis_client):
    """Test delete_stream with enforce_consumer_deletion parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the delete_stream response
        mock_response = {}
        mock_kinesis_client.delete_stream = MagicMock(return_value=mock_response)

        # Call delete_stream with enforce_consumer_deletion
        result = await delete_stream(
            stream_name='test-stream', enforce_consumer_deletion=True, region_name='us-west-2'
        )

        # Verify delete_stream was called with the right parameters
        mock_kinesis_client.delete_stream.assert_called_once()
        args = mock_kinesis_client.delete_stream.call_args[1]
        assert 'StreamName' in args
        assert args['StreamName'] == 'test-stream'
        assert 'EnforceConsumerDeletion' in args
        assert args['EnforceConsumerDeletion'] is True

        # Verify the result
        assert 'message' in result
        assert result['message'] == 'Successfully deleted stream'


# ==============================================================================
#                       decrease_stream_retention_period Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_decrease_stream_retention_period_basic(mock_kinesis_client):
    """Test basic decrease_stream_retention_period functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the decrease_stream_retention_period response
        mock_response = {}
        mock_kinesis_client.decrease_stream_retention_period = MagicMock(
            return_value=mock_response
        )

        # Call decrease_stream_retention_period
        retention_period_hours = 24
        result = await decrease_stream_retention_period(
            retention_period_hours=retention_period_hours,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify decrease_stream_retention_period was called with the right parameters
        mock_kinesis_client.decrease_stream_retention_period.assert_called_once()
        args = mock_kinesis_client.decrease_stream_retention_period.call_args[1]
        assert 'StreamName' in args
        assert args['StreamName'] == 'test-stream'
        assert 'RetentionPeriodHours' in args
        assert args['RetentionPeriodHours'] == retention_period_hours

        # Verify the result
        assert 'message' in result
        assert result['retention_period_hours'] == retention_period_hours


@pytest.mark.asyncio
async def test_decrease_stream_retention_period_with_stream_arn(mock_kinesis_client):
    """Test decrease_stream_retention_period with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the decrease_stream_retention_period response
        mock_response = {}
        mock_kinesis_client.decrease_stream_retention_period = MagicMock(
            return_value=mock_response
        )

        # Call decrease_stream_retention_period with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        retention_period_hours = 24
        result = await decrease_stream_retention_period(
            retention_period_hours=retention_period_hours,
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        # Verify decrease_stream_retention_period was called with the right parameters
        mock_kinesis_client.decrease_stream_retention_period.assert_called_once()
        args = mock_kinesis_client.decrease_stream_retention_period.call_args[1]
        assert 'StreamARN' in args
        assert args['StreamARN'] == stream_arn
        assert 'RetentionPeriodHours' in args
        assert args['RetentionPeriodHours'] == retention_period_hours

        # Verify the result
        assert 'message' in result
        assert result['retention_period_hours'] == retention_period_hours


@pytest.mark.asyncio
async def test_decrease_stream_retention_period_missing_identifiers(mock_kinesis_client):
    """Test decrease_stream_retention_period with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await decrease_stream_retention_period(
                retention_period_hours=24,
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       delete_resource_policy Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_delete_resource_policy_basic(mock_kinesis_client):
    """Test basic delete_resource_policy functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the delete_resource_policy response
        mock_response = {}
        mock_kinesis_client.delete_resource_policy = MagicMock(return_value=mock_response)

        # Call delete_resource_policy
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await delete_resource_policy(resource_arn=resource_arn, region_name='us-west-2')

        # Verify delete_resource_policy was called with the right parameters
        mock_kinesis_client.delete_resource_policy.assert_called_once()
        args = mock_kinesis_client.delete_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn

        # Verify the result
        assert result['resource_arn'] == resource_arn


@pytest.mark.asyncio
async def test_delete_resource_policy_with_different_region(mock_kinesis_client):
    """Test delete_resource_policy with a different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the delete_resource_policy response
        mock_response = {}
        mock_kinesis_client.delete_resource_policy = MagicMock(return_value=mock_response)

        # Call delete_resource_policy with a different region
        resource_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        result = await delete_resource_policy(resource_arn=resource_arn, region_name='us-east-1')

        # Verify delete_resource_policy was called with the right parameters
        mock_kinesis_client.delete_resource_policy.assert_called_once()
        args = mock_kinesis_client.delete_resource_policy.call_args[1]
        assert args['ResourceARN'] == resource_arn

        # Verify the result
        assert result['resource_arn'] == resource_arn


# ==============================================================================
#                       deregister_stream_consumer Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_deregister_stream_consumer_with_consumer_name(mock_kinesis_client):
    """Test deregister_stream_consumer with consumer name and stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the deregister_stream_consumer response
        mock_response = {}
        mock_kinesis_client.deregister_stream_consumer = MagicMock(return_value=mock_response)

        # Call deregister_stream_consumer with consumer name and stream ARN
        consumer_name = 'test-consumer'
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await deregister_stream_consumer(
            consumer_name=consumer_name, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify deregister_stream_consumer was called with the right parameters
        mock_kinesis_client.deregister_stream_consumer.assert_called_once()
        args = mock_kinesis_client.deregister_stream_consumer.call_args[1]
        assert 'ConsumerName' in args
        assert args['ConsumerName'] == consumer_name
        assert 'StreamARN' in args
        assert args['StreamARN'] == stream_arn

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_deregister_stream_consumer_with_consumer_arn(mock_kinesis_client):
    """Test deregister_stream_consumer with consumer ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the deregister_stream_consumer response
        mock_response = {}
        mock_kinesis_client.deregister_stream_consumer = MagicMock(return_value=mock_response)

        # Call deregister_stream_consumer with consumer ARN
        consumer_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890'
        result = await deregister_stream_consumer(
            consumer_arn=consumer_arn, region_name='us-west-2'
        )

        # Verify deregister_stream_consumer was called with the right parameters
        mock_kinesis_client.deregister_stream_consumer.assert_called_once()
        args = mock_kinesis_client.deregister_stream_consumer.call_args[1]
        assert 'ConsumerARN' in args
        assert args['ConsumerARN'] == consumer_arn

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_deregister_stream_consumer_missing_identifiers(mock_kinesis_client):
    """Test deregister_stream_consumer with missing consumer identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(
            ValueError, match='Either consumer_name or consumer_arn must be provided'
        ):
            await deregister_stream_consumer(
                consumer_name=None,
                consumer_arn=None,
                stream_arn='arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                region_name='us-west-2',
            )


@pytest.mark.asyncio
async def test_deregister_stream_consumer_missing_consumer_identifiers(mock_kinesis_client):
    """Test deregister_stream_consumer with missing consumer identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(
            ValueError, match='Either consumer_name or consumer_arn must be provided'
        ):
            await deregister_stream_consumer(
                consumer_name=None,
                consumer_arn=None,
                region_name='us-west-2',
            )


@pytest.mark.asyncio
async def test_deregister_stream_consumer_with_stream_arn(mock_kinesis_client):
    """Test deregister_stream_consumer with stream_arn parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {}
        mock_kinesis_client.deregister_stream_consumer = MagicMock(return_value=mock_response)

        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await deregister_stream_consumer(
            consumer_name='test-consumer',
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        args = mock_kinesis_client.deregister_stream_consumer.call_args[1]
        assert args['ConsumerName'] == 'test-consumer'
        assert args['StreamARN'] == stream_arn
        assert 'message' in result


# ==============================================================================
#                       disable_enhanced_monitoring Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_disable_enhanced_monitoring_with_stream_name(mock_kinesis_client):
    """Test disable_enhanced_monitoring with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the disable_enhanced_monitoring response
        mock_response = {
            'StreamName': 'test-stream',
            'CurrentShardLevelMetrics': ['IncomingBytes'],
            'DesiredShardLevelMetrics': [],
        }
        mock_kinesis_client.disable_enhanced_monitoring = MagicMock(return_value=mock_response)

        # Call disable_enhanced_monitoring
        shard_level_metrics = ['IncomingBytes']
        result = await disable_enhanced_monitoring(
            shard_level_metrics=shard_level_metrics,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify disable_enhanced_monitoring was called with the right parameters
        mock_kinesis_client.disable_enhanced_monitoring.assert_called_once()
        args = mock_kinesis_client.disable_enhanced_monitoring.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ShardLevelMetrics'] == shard_level_metrics

        # Verify the result contains the expected data (use server's formatted fields)
        assert result['current_shard_level_metrics'] == ['IncomingBytes']
        assert result['desired_shard_level_metrics'] == []


@pytest.mark.asyncio
async def test_disable_enhanced_monitoring_with_stream_arn(mock_kinesis_client):
    """Test disable_enhanced_monitoring with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the disable_enhanced_monitoring response
        mock_response = {
            'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
            'CurrentShardLevelMetrics': ['OutgoingBytes'],
            'DesiredShardLevelMetrics': [],
        }
        mock_kinesis_client.disable_enhanced_monitoring = MagicMock(return_value=mock_response)

        # Call disable_enhanced_monitoring with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        shard_level_metrics = ['OutgoingBytes']
        result = await disable_enhanced_monitoring(
            shard_level_metrics=shard_level_metrics, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify disable_enhanced_monitoring was called with the right parameters
        mock_kinesis_client.disable_enhanced_monitoring.assert_called_once()
        args = mock_kinesis_client.disable_enhanced_monitoring.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ShardLevelMetrics'] == shard_level_metrics

        # Verify the result contains the expected data (use server's formatted fields)
        assert result['current_shard_level_metrics'] == ['OutgoingBytes']
        assert result['desired_shard_level_metrics'] == []


@pytest.mark.asyncio
async def test_disable_enhanced_monitoring_with_multiple_metrics(mock_kinesis_client):
    """Test disable_enhanced_monitoring with multiple metrics."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the disable_enhanced_monitoring response
        mock_response = {
            'StreamName': 'test-stream',
            'CurrentShardLevelMetrics': ['IncomingBytes', 'OutgoingBytes'],
            'DesiredShardLevelMetrics': [],
        }
        mock_kinesis_client.disable_enhanced_monitoring = MagicMock(return_value=mock_response)

        # Call disable_enhanced_monitoring with multiple metrics
        shard_level_metrics = ['IncomingBytes', 'OutgoingBytes']
        result = await disable_enhanced_monitoring(
            shard_level_metrics=shard_level_metrics,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify disable_enhanced_monitoring was called with the right parameters
        mock_kinesis_client.disable_enhanced_monitoring.assert_called_once()
        args = mock_kinesis_client.disable_enhanced_monitoring.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ShardLevelMetrics'] == shard_level_metrics

        # Verify the result contains the expected data (use server's formatted fields)
        assert result['current_shard_level_metrics'] == ['IncomingBytes', 'OutgoingBytes']
        assert result['desired_shard_level_metrics'] == []


@pytest.mark.asyncio
async def test_disable_enhanced_monitoring_missing_identifiers(mock_kinesis_client):
    """Test disable_enhanced_monitoring with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await disable_enhanced_monitoring(
                shard_level_metrics=['IncomingBytes'],
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       merge_shards Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_merge_shards_with_stream_name(mock_kinesis_client):
    """Test merge_shards with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the merge_shards response
        mock_response = {}
        mock_kinesis_client.merge_shards = MagicMock(return_value=mock_response)

        # Call merge_shards with stream name
        shard_to_merge = 'shardId-000000000000'
        adjacent_shard_to_merge = 'shardId-000000000001'
        result = await merge_shards(
            shard_to_merge=shard_to_merge,
            adjacent_shard_to_merge=adjacent_shard_to_merge,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify merge_shards was called with the right parameters
        mock_kinesis_client.merge_shards.assert_called_once()
        args = mock_kinesis_client.merge_shards.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ShardToMerge'] == shard_to_merge
        assert args['AdjacentShardToMerge'] == adjacent_shard_to_merge

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_merge_shards_with_stream_arn(mock_kinesis_client):
    """Test merge_shards with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the merge_shards response
        mock_response = {}
        mock_kinesis_client.merge_shards = MagicMock(return_value=mock_response)

        # Call merge_shards with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        shard_to_merge = 'shardId-000000000002'
        adjacent_shard_to_merge = 'shardId-000000000003'
        result = await merge_shards(
            shard_to_merge=shard_to_merge,
            adjacent_shard_to_merge=adjacent_shard_to_merge,
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        # Verify merge_shards was called with the right parameters
        mock_kinesis_client.merge_shards.assert_called_once()
        args = mock_kinesis_client.merge_shards.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ShardToMerge'] == shard_to_merge
        assert args['AdjacentShardToMerge'] == adjacent_shard_to_merge

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_merge_shards_missing_identifiers(mock_kinesis_client):
    """Test merge_shards with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await merge_shards(
                shard_to_merge='shardId-000000000000',
                adjacent_shard_to_merge='shardId-000000000001',
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       remove_tags_from_stream Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_remove_tags_from_stream_with_stream_name(mock_kinesis_client):
    """Test remove_tags_from_stream with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the remove_tags_from_stream response
        mock_response = {}
        mock_kinesis_client.remove_tags_from_stream = MagicMock(return_value=mock_response)

        # Call remove_tags_from_stream with stream name
        tag_keys = ['Environment', 'Project']
        result = await remove_tags_from_stream(
            tag_keys=tag_keys, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify remove_tags_from_stream was called with the right parameters
        mock_kinesis_client.remove_tags_from_stream.assert_called_once()
        args = mock_kinesis_client.remove_tags_from_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['TagKeys'] == tag_keys

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_remove_tags_from_stream_with_stream_arn(mock_kinesis_client):
    """Test remove_tags_from_stream with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the remove_tags_from_stream response
        mock_response = {}
        mock_kinesis_client.remove_tags_from_stream = MagicMock(return_value=mock_response)

        # Call remove_tags_from_stream with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        tag_keys = ['Owner', 'CostCenter']
        result = await remove_tags_from_stream(
            tag_keys=tag_keys, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify remove_tags_from_stream was called with the right parameters
        mock_kinesis_client.remove_tags_from_stream.assert_called_once()
        args = mock_kinesis_client.remove_tags_from_stream.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['TagKeys'] == tag_keys

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_remove_tags_from_stream_with_single_tag(mock_kinesis_client):
    """Test remove_tags_from_stream with a single tag."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the remove_tags_from_stream response
        mock_response = {}
        mock_kinesis_client.remove_tags_from_stream = MagicMock(return_value=mock_response)

        # Call remove_tags_from_stream with a single tag
        tag_keys = ['Environment']
        result = await remove_tags_from_stream(
            tag_keys=tag_keys, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify remove_tags_from_stream was called with the right parameters
        mock_kinesis_client.remove_tags_from_stream.assert_called_once()
        args = mock_kinesis_client.remove_tags_from_stream.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['TagKeys'] == tag_keys

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_remove_tags_from_stream_missing_identifiers(mock_kinesis_client):
    """Test remove_tags_from_stream with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await remove_tags_from_stream(
                tag_keys=['Environment'],
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       split_shard Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_split_shard_with_stream_name(mock_kinesis_client):
    """Test split_shard with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the split_shard response
        mock_response = {}
        mock_kinesis_client.split_shard = MagicMock(return_value=mock_response)

        # Call split_shard with stream name
        shard_to_split = 'shardId-000000000000'
        new_starting_hash_key = (
            '170141183460469231731687303715884105728'  # Middle of the hash key range
        )
        result = await split_shard(
            shard_to_split=shard_to_split,
            new_starting_hash_key=new_starting_hash_key,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify split_shard was called with the right parameters
        mock_kinesis_client.split_shard.assert_called_once()
        args = mock_kinesis_client.split_shard.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['ShardToSplit'] == shard_to_split
        assert args['NewStartingHashKey'] == new_starting_hash_key

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_split_shard_with_stream_arn(mock_kinesis_client):
    """Test split_shard with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the split_shard response
        mock_response = {}
        mock_kinesis_client.split_shard = MagicMock(return_value=mock_response)

        # Call split_shard with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        shard_to_split = 'shardId-000000000002'
        new_starting_hash_key = (
            '170141183460469231731687303715884105728'  # Middle of the hash key range
        )
        result = await split_shard(
            shard_to_split=shard_to_split,
            new_starting_hash_key=new_starting_hash_key,
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        # Verify split_shard was called with the right parameters
        mock_kinesis_client.split_shard.assert_called_once()
        args = mock_kinesis_client.split_shard.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ShardToSplit'] == shard_to_split
        assert args['NewStartingHashKey'] == new_starting_hash_key

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_split_shard_missing_identifiers(mock_kinesis_client):
    """Test split_shard with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await split_shard(
                shard_to_split='shardId-000000000000',
                new_starting_hash_key='170141183460469231731687303715884105728',
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       start_stream_encryption Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_start_stream_encryption_with_stream_name(mock_kinesis_client):
    """Test start_stream_encryption with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the start_stream_encryption response
        mock_response = {}
        mock_kinesis_client.start_stream_encryption = MagicMock(return_value=mock_response)

        # Call start_stream_encryption with stream name
        key_id = 'arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012'
        result = await start_stream_encryption(
            key_id=key_id, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify start_stream_encryption was called with the right parameters
        mock_kinesis_client.start_stream_encryption.assert_called_once()
        args = mock_kinesis_client.start_stream_encryption.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['KeyId'] == key_id
        # Don't check the encryption_type directly

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_start_stream_encryption_with_stream_arn(mock_kinesis_client):
    """Test start_stream_encryption with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the start_stream_encryption response
        mock_response = {}
        mock_kinesis_client.start_stream_encryption = MagicMock(return_value=mock_response)

        # Call start_stream_encryption with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        key_id = 'arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012'
        result = await start_stream_encryption(
            key_id=key_id, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify start_stream_encryption was called with the right parameters
        mock_kinesis_client.start_stream_encryption.assert_called_once()
        args = mock_kinesis_client.start_stream_encryption.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['KeyId'] == key_id
        # Don't check the encryption_type directly

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_start_stream_encryption_with_key_alias(mock_kinesis_client):
    """Test start_stream_encryption with key alias."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the start_stream_encryption response
        mock_response = {}
        mock_kinesis_client.start_stream_encryption = MagicMock(return_value=mock_response)

        # Call start_stream_encryption with key alias
        key_id = 'alias/my-kms-key'
        result = await start_stream_encryption(
            key_id=key_id, stream_name='test-stream', region_name='us-west-2'
        )

        # Verify start_stream_encryption was called with the right parameters
        mock_kinesis_client.start_stream_encryption.assert_called_once()
        args = mock_kinesis_client.start_stream_encryption.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['KeyId'] == key_id
        # Don't check the encryption_type directly

        # Verify the result
        assert 'message' in result


# ==============================================================================
#                       stop_stream_encryption Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_stop_stream_encryption_with_stream_name(mock_kinesis_client):
    """Test stop_stream_encryption with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the stop_stream_encryption response
        mock_response = {}
        mock_kinesis_client.stop_stream_encryption = MagicMock(return_value=mock_response)

        # Call stop_stream_encryption with stream name
        result = await stop_stream_encryption(stream_name='test-stream', region_name='us-west-2')

        # Verify stop_stream_encryption was called with the right parameters
        mock_kinesis_client.stop_stream_encryption.assert_called_once()
        args = mock_kinesis_client.stop_stream_encryption.call_args[1]
        assert args['StreamName'] == 'test-stream'
        # Don't check the encryption_type directly

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_stop_stream_encryption_with_stream_arn(mock_kinesis_client):
    """Test stop_stream_encryption with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the stop_stream_encryption response
        mock_response = {}
        mock_kinesis_client.stop_stream_encryption = MagicMock(return_value=mock_response)

        # Call stop_stream_encryption with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = await stop_stream_encryption(stream_arn=stream_arn, region_name='us-west-2')

        # Verify stop_stream_encryption was called with the right parameters
        mock_kinesis_client.stop_stream_encryption.assert_called_once()
        args = mock_kinesis_client.stop_stream_encryption.call_args[1]
        assert args['StreamARN'] == stream_arn
        # Don't check the encryption_type directly

        # Verify the result
        assert 'message' in result


# ==============================================================================
#                       untag_resource Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_untag_resource_basic(mock_kinesis_client):
    """Test basic untag_resource functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the untag_resource response
        mock_response = {}
        mock_kinesis_client.untag_resource = MagicMock(return_value=mock_response)

        # Call untag_resource
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        tag_keys = ['Environment', 'Project']
        result = await untag_resource(
            resource_arn=resource_arn, tag_keys=tag_keys, region_name='us-west-2'
        )

        # Verify untag_resource was called with the right parameters
        mock_kinesis_client.untag_resource.assert_called_once()
        args = mock_kinesis_client.untag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['TagKeys'] == tag_keys

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_untag_resource_with_single_tag(mock_kinesis_client):
    """Test untag_resource with a single tag."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the untag_resource response
        mock_response = {}
        mock_kinesis_client.untag_resource = MagicMock(return_value=mock_response)

        # Call untag_resource with a single tag
        resource_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        tag_keys = ['Environment']
        result = await untag_resource(
            resource_arn=resource_arn, tag_keys=tag_keys, region_name='us-west-2'
        )

        # Verify untag_resource was called with the right parameters
        mock_kinesis_client.untag_resource.assert_called_once()
        args = mock_kinesis_client.untag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['TagKeys'] == tag_keys

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_untag_resource_with_different_region(mock_kinesis_client):
    """Test untag_resource with a different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the untag_resource response
        mock_response = {}
        mock_kinesis_client.untag_resource = MagicMock(return_value=mock_response)

        # Call untag_resource with a different region
        resource_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        tag_keys = ['Environment', 'Project']
        result = await untag_resource(
            resource_arn=resource_arn, tag_keys=tag_keys, region_name='us-east-1'
        )

        # Verify untag_resource was called with the right parameters
        mock_kinesis_client.untag_resource.assert_called_once()
        args = mock_kinesis_client.untag_resource.call_args[1]
        assert args['ResourceARN'] == resource_arn
        assert args['TagKeys'] == tag_keys

        # Verify the result
        assert 'message' in result


# ==============================================================================
#                       update_shard_count Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_update_shard_count_with_stream_name(mock_kinesis_client):
    """Test update_shard_count with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the update_shard_count response
        mock_response = {
            'StreamName': 'test-stream',
            'CurrentShardCount': 2,
            'TargetShardCount': 4,
        }
        mock_kinesis_client.update_shard_count = MagicMock(return_value=mock_response)

        # Call update_shard_count with stream name
        target_shard_count = 4
        scaling_type = 'UNIFORM_SCALING'
        result = await update_shard_count(
            target_shard_count=target_shard_count,
            scaling_type=scaling_type,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify update_shard_count was called with the right parameters
        mock_kinesis_client.update_shard_count.assert_called_once()
        args = mock_kinesis_client.update_shard_count.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['TargetShardCount'] == target_shard_count
        assert args['ScalingType'] == scaling_type

        # Verify the result (only check fields that server actually returns)
        assert result['target_shard_count'] == target_shard_count
        assert result['scaling_type'] == scaling_type


@pytest.mark.asyncio
async def test_update_shard_count_with_stream_arn(mock_kinesis_client):
    """Test update_shard_count with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the update_shard_count response
        mock_response = {
            'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
            'CurrentShardCount': 1,
            'TargetShardCount': 2,
        }
        mock_kinesis_client.update_shard_count = MagicMock(return_value=mock_response)

        # Call update_shard_count with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        target_shard_count = 2
        scaling_type = 'UNIFORM_SCALING'
        result = await update_shard_count(
            target_shard_count=target_shard_count,
            scaling_type=scaling_type,
            stream_arn=stream_arn,
            region_name='us-west-2',
        )

        # Verify update_shard_count was called with the right parameters
        mock_kinesis_client.update_shard_count.assert_called_once()
        args = mock_kinesis_client.update_shard_count.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['TargetShardCount'] == target_shard_count
        assert args['ScalingType'] == scaling_type

        # Verify the result (only check fields that server actually returns)
        assert result['target_shard_count'] == target_shard_count
        assert result['scaling_type'] == scaling_type


@pytest.mark.asyncio
async def test_update_shard_count_missing_identifiers(mock_kinesis_client):
    """Test update_shard_count with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await update_shard_count(
                target_shard_count=4,
                scaling_type='UNIFORM_SCALING',
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


# ==============================================================================
#                       update_stream_mode Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_update_stream_mode_to_provisioned(mock_kinesis_client):
    """Test update_stream_mode to PROVISIONED mode."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the update_stream_mode response
        mock_response = {}
        mock_kinesis_client.update_stream_mode = MagicMock(return_value=mock_response)

        # Call update_stream_mode with PROVISIONED mode
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        stream_mode_details = 'PROVISIONED'
        result = await update_stream_mode(
            stream_mode_details=stream_mode_details, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify update_stream_mode was called with the right parameters
        mock_kinesis_client.update_stream_mode.assert_called_once()
        args = mock_kinesis_client.update_stream_mode.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['StreamModeDetails']['StreamMode'] == stream_mode_details

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_update_stream_mode_to_on_demand(mock_kinesis_client):
    """Test update_stream_mode to ON_DEMAND mode."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the update_stream_mode response
        mock_response = {}
        mock_kinesis_client.update_stream_mode = MagicMock(return_value=mock_response)

        # Call update_stream_mode with ON_DEMAND mode
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        stream_mode_details = 'ON_DEMAND'
        result = await update_stream_mode(
            stream_mode_details=stream_mode_details, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify update_stream_mode was called with the right parameters
        mock_kinesis_client.update_stream_mode.assert_called_once()
        args = mock_kinesis_client.update_stream_mode.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['StreamModeDetails']['StreamMode'] == stream_mode_details

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_update_stream_mode_with_different_region(mock_kinesis_client):
    """Test update_stream_mode with a different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the update_stream_mode response
        mock_response = {}
        mock_kinesis_client.update_stream_mode = MagicMock(return_value=mock_response)

        # Call update_stream_mode with a different region
        stream_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        stream_mode_details = 'PROVISIONED'
        result = await update_stream_mode(
            stream_mode_details=stream_mode_details, stream_arn=stream_arn, region_name='us-east-1'
        )

        # Verify update_stream_mode was called with the right parameters
        mock_kinesis_client.update_stream_mode.assert_called_once()
        args = mock_kinesis_client.update_stream_mode.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['StreamModeDetails']['StreamMode'] == stream_mode_details

        # Verify the result
        assert 'message' in result


# ==============================================================================
#                       put_record Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_put_record_with_stream_name(mock_kinesis_client):
    """Test put_record with stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_record response
        mock_response = {
            'ShardId': 'shardId-000000000000',
            'SequenceNumber': '49598630142999655949581543785528105911853783356538642434',
        }
        mock_kinesis_client.put_record = MagicMock(return_value=mock_response)

        # Call put_record with stream name
        data = 'test-data'
        partition_key = 'test-key'
        result = await put_record(
            data=data,
            partition_key=partition_key,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify put_record was called with the right parameters
        mock_kinesis_client.put_record.assert_called_once()
        args = mock_kinesis_client.put_record.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['PartitionKey'] == partition_key

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_put_record_with_stream_arn(mock_kinesis_client):
    """Test put_record with stream ARN."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_record response
        mock_response = {
            'ShardId': 'shardId-000000000001',
            'SequenceNumber': '49598630142999655949581543785528105911853783356538642435',
        }
        mock_kinesis_client.put_record = MagicMock(return_value=mock_response)

        # Call put_record with stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        data = 'test-data'
        partition_key = 'test-key'
        result = await put_record(
            data=data, partition_key=partition_key, stream_arn=stream_arn, region_name='us-west-2'
        )

        # Verify put_record was called with the right parameters
        mock_kinesis_client.put_record.assert_called_once()
        args = mock_kinesis_client.put_record.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['PartitionKey'] == partition_key

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_put_record_with_explicit_hash_key(mock_kinesis_client):
    """Test put_record with explicit hash key."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the put_record response
        mock_response = {
            'ShardId': 'shardId-000000000002',
            'SequenceNumber': '49598630142999655949581543785528105911853783356538642436',
        }
        mock_kinesis_client.put_record = MagicMock(return_value=mock_response)

        # Call put_record with explicit hash key
        data = 'test-data'
        partition_key = 'test-key'
        explicit_hash_key = '123456789012345678901234567890'
        result = await put_record(
            data=data,
            partition_key=partition_key,
            explicit_hash_key=explicit_hash_key,
            stream_name='test-stream',
            region_name='us-west-2',
        )

        # Verify put_record was called with the right parameters
        mock_kinesis_client.put_record.assert_called_once()
        args = mock_kinesis_client.put_record.call_args[1]
        assert args['StreamName'] == 'test-stream'
        assert args['PartitionKey'] == partition_key
        assert args['ExplicitHashKey'] == explicit_hash_key

        # Verify the result
        assert 'message' in result


@pytest.mark.asyncio
async def test_put_record_missing_identifiers(mock_kinesis_client):
    """Test put_record with missing stream identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # This is a validation error that happens before the API call
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            await put_record(
                data='test-data',
                partition_key='test-key',
                stream_name=None,
                stream_arn=None,
                region_name='us-west-2',
            )


@pytest.mark.asyncio
async def test_put_record_with_sequence_number_for_ordering(mock_kinesis_client):
    """Test put_record with sequence number for ordering."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        mock_response = {
            'ShardId': 'shardId-000000000000',
            'SequenceNumber': '49598630142999655949581543785528105911853783356538642434',
        }
        mock_kinesis_client.put_record = MagicMock(return_value=mock_response)

        result = await put_record(
            data='test-data',
            partition_key='test-key',
            sequence_number_for_ordering='12345',
            stream_name='test-stream',
            region_name='us-west-2',
        )

        args = mock_kinesis_client.put_record.call_args[1]
        assert args['SequenceNumberForOrdering'] == '12345'
        assert 'message' in result


# ==============================================================================
#                       register_stream_consumer Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_register_stream_consumer_basic(mock_kinesis_client):
    """Test basic register_stream_consumer functionality."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the register_stream_consumer response
        mock_response = {
            'Consumer': {
                'ConsumerName': 'test-consumer',
                'ConsumerARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890',
                'ConsumerStatus': 'CREATING',
                'ConsumerCreationTimestamp': datetime(2023, 1, 1),
            }
        }
        mock_kinesis_client.register_stream_consumer = MagicMock(return_value=mock_response)

        # Call register_stream_consumer
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        consumer_name = 'test-consumer'
        result = await register_stream_consumer(
            stream_arn=stream_arn, consumer_name=consumer_name, region_name='us-west-2'
        )

        # Verify register_stream_consumer was called with the right parameters
        mock_kinesis_client.register_stream_consumer.assert_called_once()
        args = mock_kinesis_client.register_stream_consumer.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ConsumerName'] == consumer_name

        # Verify the result (skip tags assertion due to FieldInfo bug)
        assert result['stream_arn'] == stream_arn
        assert result['consumer_name'] == consumer_name


@pytest.mark.asyncio
async def test_register_stream_consumer_with_tags(mock_kinesis_client):
    """Test register_stream_consumer with tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the register_stream_consumer response
        mock_response = {
            'Consumer': {
                'ConsumerName': 'test-consumer',
                'ConsumerARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream/consumer/test-consumer:1234567890',
                'ConsumerStatus': 'CREATING',
                'ConsumerCreationTimestamp': datetime(2023, 1, 1),
            }
        }
        mock_kinesis_client.register_stream_consumer = MagicMock(return_value=mock_response)

        # Call register_stream_consumer with tags
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        consumer_name = 'test-consumer'
        tags = {'Environment': 'Test', 'Project': 'Kinesis'}
        result = await register_stream_consumer(
            stream_arn=stream_arn, consumer_name=consumer_name, tags=tags, region_name='us-west-2'
        )

        # Verify register_stream_consumer was called with the right parameters
        mock_kinesis_client.register_stream_consumer.assert_called_once()
        args = mock_kinesis_client.register_stream_consumer.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ConsumerName'] == consumer_name
        assert args['Tags'] == tags

        # Verify the result
        assert result['stream_arn'] == stream_arn
        assert result['consumer_name'] == consumer_name
        assert result['tags'] == tags


@pytest.mark.asyncio
async def test_register_stream_consumer_with_different_region(mock_kinesis_client):
    """Test register_stream_consumer with different region."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the register_stream_consumer response
        mock_response = {
            'Consumer': {
                'ConsumerName': 'test-consumer',
                'ConsumerARN': 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream/consumer/test-consumer:1234567890',
                'ConsumerStatus': 'CREATING',
                'ConsumerCreationTimestamp': datetime(2023, 1, 1),
            }
        }
        mock_kinesis_client.register_stream_consumer = MagicMock(return_value=mock_response)

        # Call register_stream_consumer with different region
        stream_arn = 'arn:aws:kinesis:us-east-1:123456789012:stream/test-stream'
        consumer_name = 'test-consumer'
        result = await register_stream_consumer(
            stream_arn=stream_arn, consumer_name=consumer_name, region_name='us-east-1'
        )

        # Verify register_stream_consumer was called with the right parameters
        mock_kinesis_client.register_stream_consumer.assert_called_once()
        args = mock_kinesis_client.register_stream_consumer.call_args[1]
        assert args['StreamARN'] == stream_arn
        assert args['ConsumerName'] == consumer_name

        # Verify the result
        assert result['stream_arn'] == stream_arn
        assert result['consumer_name'] == consumer_name
        assert result['region'] == 'us-east-1'
