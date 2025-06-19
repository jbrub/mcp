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
from moto import mock_aws
from unittest.mock import MagicMock, patch


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from awslabs.kinesis_mcp_server.consts import (
    MAX_LENGTH_SHARD_ITERATOR,
    MAX_LIMIT,
    MAX_RECORDS,
    MAX_STREAM_ARN_LENGTH,
    MAX_STREAM_NAME_LENGTH,
    MAX_TAG_KEY_LENGTH,
    MAX_TAG_VALUE_LENGTH,
    MAX_TAGS_COUNT,
    MIN_RECORDS,
    STREAM_MODE_ON_DEMAND,
)
from awslabs.kinesis_mcp_server.server import (
    create_stream,
    describe_stream_summary,
    get_records,
    list_streams,
    put_records,
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


"""put_records Error Tests"""


def test_put_records_missing_records(mock_kinesis_client):
    """Test put_records with missing records parameter."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='records is required'):
            put_records(stream_name='test-stream', records=[], region_name='us-west-2')


def test_put_records_invalid_records_type(mock_kinesis_client):
    """Test put_records with invalid records type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='records must be a list'):
            put_records(stream_name='test-stream', records='not-a-list', region_name='us-west-2')


def test_put_records_too_many_records(mock_kinesis_client):
    """Test put_records with too many records."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create more records than MAX_RECORDS
        records = [
            {'Data': f'data-{i}', 'PartitionKey': f'key-{i}'} for i in range(MAX_RECORDS + 1)
        ]
        with pytest.raises(
            ValueError, match=f'Number of records must be between {MIN_RECORDS} and {MAX_RECORDS}'
        ):
            put_records(stream_name='test-stream', records=records, region_name='us-west-2')


def test_put_records_missing_stream_identifier(mock_kinesis_client):
    """Test put_records with missing stream identifier."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        records = [{'Data': 'test', 'PartitionKey': 'key'}]
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            put_records(
                records=records, stream_name=None, stream_arn=None, region_name='us-west-2'
            )


def test_put_records_invalid_stream_name_length(mock_kinesis_client):
    """Test put_records with invalid stream name length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        records = [{'Data': 'test', 'PartitionKey': 'key'}]
        # Create a stream name that's too long
        long_name = 'a' * (MAX_STREAM_NAME_LENGTH + 1)
        with pytest.raises(ValueError, match='stream_name length must be between'):
            put_records(stream_name=long_name, records=records, region_name='us-west-2')


def test_put_records_with_stream_arn(mock_kinesis_client):
    """Test put_records with stream ARN instead of name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        records = [{'Data': 'test', 'PartitionKey': 'key'}]
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'

        # Mock the put_records response
        mock_response = {
            'Records': [{'SequenceNumber': '123', 'ShardId': 'shardId-000000000000'}],
            'FailedRecordCount': 0,
        }
        mock_kinesis_client.put_records = MagicMock(return_value=mock_response)

        result = put_records(stream_arn=stream_arn, records=records, region_name='us-west-2')

        assert result['FailedRecordCount'] == 0
        assert len(result['Records']) == 1
        mock_kinesis_client.put_records.assert_called_with(Records=records, StreamARN=stream_arn)


def test_put_records_invalid_stream_arn_length(mock_kinesis_client):
    """Test put_records with invalid stream ARN length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        records = [{'Data': 'test', 'PartitionKey': 'key'}]
        # Create a stream ARN that's too long
        long_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/' + 'a' * (MAX_STREAM_ARN_LENGTH)
        with pytest.raises(ValueError, match='stream_arn length must be between'):
            put_records(stream_arn=long_arn, records=records, region_name='us-west-2')


def test_put_records_invalid_stream_arn_type(mock_kinesis_client):
    """Test put_records with invalid stream ARN type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        records = [{'Data': 'test', 'PartitionKey': 'key'}]
        with pytest.raises(TypeError, match='stream_arn must be a string'):
            put_records(stream_arn=123, records=records, region_name='us-west-2')


def test_put_records_invalid_stream_name_type(mock_kinesis_client):
    """Test put_records with invalid stream name type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        records = [{'Data': 'test', 'PartitionKey': 'key'}]
        with pytest.raises(TypeError, match='stream_name must be a string'):
            put_records(stream_name=123, records=records, region_name='us-west-2')


"""get_records Error Tests"""


def test_get_records_missing_shard_iterator(mock_kinesis_client):
    """Test get_records with missing shard iterator."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='shard_iterator is required'):
            get_records(shard_iterator='', region_name='us-west-2')


def test_get_records_invalid_shard_iterator_length(mock_kinesis_client):
    """Test get_records with invalid shard iterator length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a shard iterator that's too long
        long_iterator = 'a' * (MAX_LENGTH_SHARD_ITERATOR + 1)
        with pytest.raises(ValueError, match='shard_iterator length must be between'):
            get_records(shard_iterator=long_iterator, region_name='us-west-2')


def test_get_records_invalid_limit_value(mock_kinesis_client):
    """Test get_records with invalid limit."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='limit must be between'):
            get_records(
                shard_iterator='valid-iterator', limit=MAX_LIMIT + 1, region_name='us-west-2'
            )


def test_get_records_invalid_limit_type(mock_kinesis_client):
    """Test get_records with invalid limit type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='limit must be an integer'):
            get_records(
                shard_iterator='valid-iterator', limit='not-an-int', region_name='us-west-2'
            )


def test_get_records_with_limit(mock_kinesis_client):
    """Test get_records with a specific limit."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a test stream
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Get a shard iterator
        shard_response = mock_kinesis_client.describe_stream(StreamName='test-stream')
        shard_id = shard_response['StreamDescription']['Shards'][0]['ShardId']
        iterator_response = mock_kinesis_client.get_shard_iterator(
            StreamName='test-stream', ShardId=shard_id, ShardIteratorType='TRIM_HORIZON'
        )
        shard_iterator = iterator_response['ShardIterator']

        # Mock the get_records response
        mock_response = {
            'Records': [],
            'NextShardIterator': 'next-iterator',
            'MillisBehindLatest': 0,
        }
        mock_kinesis_client.get_records = MagicMock(return_value=mock_response)

        # Call get_records with a limit
        limit = 10
        get_records(shard_iterator=shard_iterator, limit=limit, region_name='us-west-2')

        # Verify the limit was passed correctly
        mock_kinesis_client.get_records.assert_called_with(
            ShardIterator=shard_iterator, Limit=limit
        )


def test_get_records_invalid_stream_arn_length(mock_kinesis_client):
    """Test get_records with invalid stream ARN length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a stream ARN that's too long
        long_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/' + 'a' * (MAX_STREAM_ARN_LENGTH)
        with pytest.raises(ValueError, match='stream_arn length must be between'):
            get_records(
                shard_iterator='valid-iterator', stream_arn=long_arn, region_name='us-west-2'
            )


"""create_stream Error Tests"""


def test_create_stream_missing_stream_name(mock_kinesis_client):
    """Test create_stream with missing stream name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='stream_name is required'):
            create_stream(stream_name='', region_name='us-west-2')


def test_create_stream_invalid_stream_name_length(mock_kinesis_client):
    """Test create_stream with invalid stream name length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a stream name that's too long
        long_name = 'a' * (MAX_STREAM_NAME_LENGTH + 1)
        with pytest.raises(ValueError, match='stream_name length must be between'):
            create_stream(stream_name=long_name, region_name='us-west-2')


def test_create_stream_invalid_stream_name_type(mock_kinesis_client):
    """Test create_stream with invalid stream name type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='stream_name must be a string'):
            create_stream(stream_name=123, region_name='us-west-2')


def test_create_stream_invalid_shard_count_type(mock_kinesis_client):
    """Test create_stream with invalid shard count type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='shard_count must be an integer'):
            create_stream(
                stream_name='test-stream', shard_count='not-an-int', region_name='us-west-2'
            )


def test_create_stream_invalid_shard_count(mock_kinesis_client):
    """Test create_stream with invalid shard count."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='shard_count must be between'):
            create_stream(stream_name='test-stream', shard_count=0, region_name='us-west-2')


def test_create_stream_invalid_tags(mock_kinesis_client):
    """Test create_stream with invalid tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='tags must be a dictionary'):
            create_stream(stream_name='test-stream', tags='not-a-dict', region_name='us-west-2')


def test_create_stream_too_many_tags(mock_kinesis_client):
    """Test create_stream with too many tags."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create more tags than MAX_TAGS_COUNT
        tags = {f'key-{i}': f'value-{i}' for i in range(MAX_TAGS_COUNT + 1)}
        with pytest.raises(ValueError, match='Number of tags cannot exceed'):
            create_stream(stream_name='test-stream', tags=tags, region_name='us-west-2')


def test_create_stream_invalid_tag_key_length(mock_kinesis_client):
    """Test create_stream with invalid tag key length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a tag key that's too long
        long_key = 'a' * (MAX_TAG_KEY_LENGTH + 1)
        tags = {long_key: 'value'}
        with pytest.raises(ValueError, match='Tag key length must be between'):
            create_stream(stream_name='test-stream', tags=tags, region_name='us-west-2')


def test_create_stream_invalid_tag_value_length(mock_kinesis_client):
    """Test create_stream with invalid tag value length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a tag value that's too long
        long_value = 'a' * (MAX_TAG_VALUE_LENGTH + 1)
        tags = {'key': long_value}
        with pytest.raises(ValueError, match='Tag value length cannot exceed'):
            create_stream(stream_name='test-stream', tags=tags, region_name='us-west-2')


def test_create_stream_with_on_demand_mode(mock_kinesis_client):
    """Test creating a stream with on-demand mode."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Mock the create_stream method
        mock_kinesis_client.create_stream = MagicMock()

        # Call create_stream with on-demand mode
        stream_name = 'test-stream'
        stream_mode_details = {'StreamMode': STREAM_MODE_ON_DEMAND}
        create_stream(
            stream_name=stream_name,
            stream_mode_details=stream_mode_details,
            region_name='us-west-2',
        )

        # Verify that ShardCount was not passed to create_stream when using ON_DEMAND mode
        mock_kinesis_client.create_stream.assert_called_once()
        call_kwargs = mock_kinesis_client.create_stream.call_args[1]
        assert 'StreamName' in call_kwargs
        assert 'StreamModeDetails' in call_kwargs
        assert 'ShardCount' not in call_kwargs


def test_create_stream_invalid_stream_mode_details_type(mock_kinesis_client):
    """Test create_stream with invalid stream mode details type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='stream_mode_details must be a dictionary'):
            create_stream(
                stream_name='test-stream',
                stream_mode_details='not-a-dict',
                region_name='us-west-2',
            )


"""list_streams Error Tests"""


def test_list_streams_invalid_limit(mock_kinesis_client):
    """Test list_streams with invalid limit."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='limit must be between'):
            list_streams(limit=0, region_name='us-west-2')


def test_list_streams_invalid_exclusive_start_stream_name(mock_kinesis_client):
    """Test list_streams with invalid exclusive_start_stream_name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a stream name that's too long
        long_name = 'a' * (MAX_STREAM_NAME_LENGTH + 1)
        with pytest.raises(ValueError, match='exclusive_start_stream_name length must be between'):
            list_streams(exclusive_start_stream_name=long_name, region_name='us-west-2')


def test_list_streams_invalid_exclusive_start_stream_name_type(mock_kinesis_client):
    """Test list_streams with invalid exclusive_start_stream_name type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='exclusive_start_stream_name must be a string'):
            list_streams(exclusive_start_stream_name=123, region_name='us-west-2')


def test_list_streams_invalid_limit_type(mock_kinesis_client):
    """Test list_streams with invalid limit type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='limit must be an integer'):
            list_streams(limit='not-an-int', region_name='us-west-2')


def test_list_streams_invalid_next_token_type(mock_kinesis_client):
    """Test list_streams with invalid next_token type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='next_token must be a string'):
            list_streams(next_token=123, region_name='us-west-2')


"""describe_stream_summary Error Tests"""


def test_describe_stream_summary_missing_identifiers(mock_kinesis_client):
    """Test describe_stream_summary with missing identifiers."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(ValueError, match='Either stream_name or stream_arn must be provided'):
            describe_stream_summary(stream_name=None, stream_arn=None, region_name='us-west-2')


def test_describe_stream_summary_invalid_stream_name_length(mock_kinesis_client):
    """Test describe_stream_summary with invalid stream name length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a stream name that's too long
        long_name = 'a' * (MAX_STREAM_NAME_LENGTH + 1)
        with pytest.raises(ValueError, match='stream_name length must be between'):
            describe_stream_summary(stream_name=long_name, region_name='us-west-2')


def test_describe_stream_summary_with_stream_arn(mock_kinesis_client):
    """Test describe_stream_summary with stream ARN instead of name."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a test stream
        mock_kinesis_client.create_stream(StreamName='test-stream', ShardCount=1)

        # Mock the describe_stream_summary response
        mock_response = {
            'StreamDescriptionSummary': {
                'StreamName': 'test-stream',
                'StreamARN': 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream',
                'StreamStatus': 'ACTIVE',
                'OpenShardCount': 1,
            }
        }
        mock_kinesis_client.describe_stream_summary = MagicMock(return_value=mock_response)

        # Call describe_stream_summary with a stream ARN
        stream_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/test-stream'
        result = describe_stream_summary(stream_arn=stream_arn, region_name='us-west-2')

        # Verify the ARN was passed correctly
        mock_kinesis_client.describe_stream_summary.assert_called_with(StreamARN=stream_arn)
        assert result['StreamDescriptionSummary']['StreamName'] == 'test-stream'


def test_describe_stream_summary_invalid_stream_name_type(mock_kinesis_client):
    """Test describe_stream_summary with invalid stream name type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='stream_name must be a string'):
            describe_stream_summary(stream_name=123, region_name='us-west-2')


def test_describe_stream_summary_invalid_stream_arn_type(mock_kinesis_client):
    """Test describe_stream_summary with invalid stream ARN type."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        with pytest.raises(TypeError, match='stream_arn must be a string'):
            describe_stream_summary(stream_arn=123, region_name='us-west-2')


def test_describe_stream_summary_invalid_stream_arn_length(mock_kinesis_client):
    """Test describe_stream_summary with invalid stream ARN length."""
    with patch(
        'awslabs.kinesis_mcp_server.server.get_kinesis_client', return_value=mock_kinesis_client
    ):
        # Create a stream ARN that's too long
        long_arn = 'arn:aws:kinesis:us-west-2:123456789012:stream/' + 'a' * (MAX_STREAM_ARN_LENGTH)
        with pytest.raises(ValueError, match='stream_arn length must be between'):
            describe_stream_summary(stream_arn=long_arn, region_name='us-west-2')
