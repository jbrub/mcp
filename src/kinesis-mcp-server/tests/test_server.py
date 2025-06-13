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


# /home/jbrub/.local/share/mise/installs/python/3.12.10/bin/python -m pytest /home/jbrub/workplace/mcp/src/kinesis-mcp-server/tests/test_server.py -v
"""Tests for the kinesis MCP Server."""

import boto3
import os
import pytest
import sys
from moto import mock_aws
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from awslabs.kinesis_mcp_server.server import (
    create_stream,
    describe_stream_summary,
    list_streams,
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
# Create a mock for the mcp module


@pytest.fixture
def mock_kinesis_client():
    """Create a mock Kinesis client using moto."""
    with mock_aws():
        client = boto3.client('kinesis', region_name='us-east-1')
        yield client


"""create_stream Tests"""


def test_create_stream_provisioned_mode(mock_kinesis_client):
    """Test creating a stream with provisioned mode."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the actual create_stream function
        result = create_stream(
            stream_name='test-stream',
            shard_count=2,
            stream_mode_details={'StreamMode': 'PROVISIONED'},
        )

        # Verify the result
        assert result['StreamName'] == 'test-stream'
        assert result['ShardCount'] == 2
        assert result['StreamModeDetails'] == {'StreamMode': 'PROVISIONED'}
        assert result['Status'] == 'CREATING'

        # Verify the stream was created in mock AWS
        response = mock_kinesis_client.list_streams()
        assert 'test-stream' in response['StreamNames']


def test_create_stream_on_demand_mode(mock_kinesis_client):
    """Test creating a stream with on-demand mode."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the actual create_stream function
        result = create_stream(
            stream_name='test-stream-ondemand', stream_mode_details={'StreamMode': 'ON_DEMAND'}
        )

        # Verify the result
        assert result['StreamName'] == 'test-stream-ondemand'
        assert result['StreamModeDetails'] == {'StreamMode': 'ON_DEMAND'}
        assert result['Status'] == 'CREATING'

        # Verify the stream was created in mock AWS
        response = mock_kinesis_client.list_streams()
        assert 'test-stream-ondemand' in response['StreamNames']


def test_create_stream_with_tags(mock_kinesis_client):
    """Test creating a stream with tags."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the actual create_stream function
        result = create_stream(stream_name='test-stream-tags', tags={'Environment': 'Production'})

        # Verify the result
        assert result['StreamName'] == 'test-stream-tags'
        assert result['Tags'] == {'Environment': 'Production'}
        assert result['Status'] == 'CREATING'

        # Verify the stream was created in mock AWS
        response = mock_kinesis_client.list_streams()
        assert 'test-stream-tags' in response['StreamNames']


def test_create_stream_with_max_shards(mock_kinesis_client):
    """Test creating a stream with maximum number of shards."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the actual create_stream function
        result = create_stream(stream_name='test-stream-max-shards', shard_count=100)

        # Verify the result
        assert result['StreamName'] == 'test-stream-max-shards'
        assert result['ShardCount'] == 100
        assert result['Status'] == 'CREATING'

        # Verify the stream was created in mock AWS
        response = mock_kinesis_client.list_streams()
        assert 'test-stream-max-shards' in response['StreamNames']


def test_create_stream_with_min_shards(mock_kinesis_client):
    """Test creating a stream with minimum number of shards."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the actual create_stream function
        result = create_stream(stream_name='test-stream-min-shards', shard_count=1)

        # Verify the result
        assert result['StreamName'] == 'test-stream-min-shards'
        assert result['ShardCount'] == 1
        assert result['Status'] == 'CREATING'

        # Verify the stream was created in mock AWS
        response = mock_kinesis_client.list_streams()
        assert 'test-stream-min-shards' in response['StreamNames']


def test_create_stream_with_invalid_shards(mock_kinesis_client):
    """Test creating a stream with invalid shard count."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the function with invalid_shards
        result = create_stream(stream_name='test-stream-invalid-shards', shard_count=0)

        # The handle_exceptions decorator will return None on error
        assert result is None


def test_create_stream_with_invalid_name(mock_kinesis_client):
    """Test creating a stream with invalid name."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the function with invalid_name
        result = create_stream(stream_name='', shard_count=2)

        # The handle_exceptions decorator will return None on error
        assert result is None


"""list_streams Tests"""


def test_list_streams_empty(mock_kinesis_client):
    """Test listing streams when no streams exist."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the list_streams function with no streams created
        result = list_streams()

        # Verify the result has an empty StreamNames list
        assert 'StreamNames' in result
        assert len(result['StreamNames']) == 0


def test_list_streams_with_streams(mock_kinesis_client):
    """Test listing streams when multiple streams exist."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create some test streams first
        mock_kinesis_client.create_stream(StreamName='test-stream-1', ShardCount=1)
        mock_kinesis_client.create_stream(StreamName='test-stream-2', ShardCount=1)
        mock_kinesis_client.create_stream(StreamName='test-stream-3', ShardCount=1)

        # Call the list_streams function
        result = list_streams()

        # Verify the result contains the created streams
        assert 'StreamNames' in result
        assert len(result['StreamNames']) == 3
        assert 'test-stream-1' in result['StreamNames']
        assert 'test-stream-2' in result['StreamNames']
        assert 'test-stream-3' in result['StreamNames']


def test_list_streams_with_limit(mock_kinesis_client):
    """Test that list_streams respects the limit parameter."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create some test streams first
        mock_kinesis_client.create_stream(StreamName='test-stream-1', ShardCount=1)
        mock_kinesis_client.create_stream(StreamName='test-stream-2', ShardCount=1)
        mock_kinesis_client.create_stream(StreamName='test-stream-3', ShardCount=1)

        # Call the list_streams function with a limit
        result = list_streams(limit=2)

        # Verify the result respects the limit
        assert 'StreamNames' in result
        assert len(result['StreamNames']) <= 2


def test_list_streams_with_exclusive_start_stream_name(mock_kinesis_client):
    """Test listing streams with an exclusive start stream name."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create some test streams first (in alphabetical order)
        mock_kinesis_client.create_stream(StreamName='a-test-stream', ShardCount=1)
        mock_kinesis_client.create_stream(StreamName='b-test-stream', ShardCount=1)
        mock_kinesis_client.create_stream(StreamName='c-test-stream', ShardCount=1)

        # Call the list_streams function with an exclusive start stream name
        result = list_streams(exclusive_start_stream_name='a-test-stream')

        # Verify the result starts after the specified stream
        assert 'StreamNames' in result
        assert 'a-test-stream' not in result['StreamNames']
        assert 'b-test-stream' in result['StreamNames']
        assert 'c-test-stream' in result['StreamNames']


"""describe_stream_summary Tests"""


def test_describe_stream_summary_with_stream(mock_kinesis_client):
    """Test describing a stream summary for an existing stream."""
    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create a test stream first
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Call the describe_stream_summary function
        result = describe_stream_summary(stream_name='test-stream')

        # Verify the result contains the stream details
        assert 'StreamDescriptionSummary' in result
        assert 'StreamName' in result['StreamDescriptionSummary']
        assert result['StreamDescriptionSummary']['StreamName'] == 'test-stream'
        assert 'OpenShardCount' in result['StreamDescriptionSummary']
        assert result['StreamDescriptionSummary']['OpenShardCount'] == 1


"""put_records Tests"""


def test_put_records_basic(mock_kinesis_client):
    """Test putting records to a stream."""
    from awslabs.kinesis_mcp_server.server import put_records

    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create a test stream first
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Create test records
        records = [
            {'Data': 'test data 1', 'PartitionKey': 'partition-1'},
            {'Data': 'test data 2', 'PartitionKey': 'partition-2'},
        ]

        # Call the put_records function
        result = put_records(stream_name='test-stream', records=records)

        # Verify the result contains the expected fields
        assert 'Records' in result
        assert len(result['Records']) == 2
        assert 'ShardId' in result['Records'][0]
        assert 'SequenceNumber' in result['Records'][0]


def test_put_records_with_stream_arn(mock_kinesis_client):
    """Test putting records to a stream using stream ARN."""
    from awslabs.kinesis_mcp_server.server import put_records

    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create a test stream first
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Create test records
        records = [{'Data': 'test data 1', 'PartitionKey': 'partition-1'}]

        # Call the put_records function with stream ARN
        result = put_records(
            stream_name=None,
            records=records,
            stream_arn='arn:aws:kinesis:us-east-1:123456789012:stream/test-stream',
        )

        # Verify the result contains the expected fields
        assert 'Records' in result
        assert len(result['Records']) == 1


def test_put_records_with_invalid_stream(mock_kinesis_client):
    """Test putting records to a non-existent stream."""
    from awslabs.kinesis_mcp_server.server import put_records

    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create test records
        records = [{'Data': 'test data 1', 'PartitionKey': 'partition-1'}]

        # Call the put_records function with non-existent stream
        result = put_records(stream_name='non-existent-stream', records=records)

        # The handle_exceptions decorator will return None on error
        assert result is None


"""get_records Tests"""


def test_get_records_basic(mock_kinesis_client):
    """Test getting records from a shard."""
    from awslabs.kinesis_mcp_server.server import get_records

    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create a test stream first
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Get a shard iterator
        shard_response = mock_kinesis_client.describe_stream(StreamName='test-stream')
        shard_id = shard_response['StreamDescription']['Shards'][0]['ShardId']
        iterator_response = mock_kinesis_client.get_shard_iterator(
            StreamName='test-stream', ShardId=shard_id, ShardIteratorType='TRIM_HORIZON'
        )
        shard_iterator = iterator_response['ShardIterator']

        # Call the get_records function
        result = get_records(shard_iterator=shard_iterator)

        # Verify the result contains the expected fields
        assert 'Records' in result
        assert 'NextShardIterator' in result
        assert 'MillisBehindLatest' in result


def test_get_records_with_limit(mock_kinesis_client):
    """Test getting records with a limit parameter."""
    from awslabs.kinesis_mcp_server.server import get_records

    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Create a test stream first
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Get a shard iterator
        shard_response = mock_kinesis_client.describe_stream(StreamName='test-stream')
        shard_id = shard_response['StreamDescription']['Shards'][0]['ShardId']
        iterator_response = mock_kinesis_client.get_shard_iterator(
            StreamName='test-stream', ShardId=shard_id, ShardIteratorType='TRIM_HORIZON'
        )
        shard_iterator = iterator_response['ShardIterator']

        # Call the get_records function with limit
        result = get_records(shard_iterator=shard_iterator, limit=10)

        # Verify the result contains the expected fields
        assert 'Records' in result
        assert 'NextShardIterator' in result
        assert 'MillisBehindLatest' in result


def test_get_records_with_invalid_iterator(mock_kinesis_client):
    """Test getting records with an invalid shard iterator."""
    from awslabs.kinesis_mcp_server.server import get_records

    # Patch the kinesis client in the server module
    with patch('awslabs.kinesis_mcp_server.server.kinesis', mock_kinesis_client):
        # Call the get_records function with invalid iterator
        result = get_records(shard_iterator='invalid-iterator')

        # The handle_exceptions decorator will return None on error
        assert result is None
