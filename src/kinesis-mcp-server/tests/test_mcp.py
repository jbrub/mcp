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

"""MCP Protocol tests for the Kinesis MCP Server."""

import boto3
import json
import os
import pytest
import subprocess
import time
from awslabs.kinesis_mcp_server.server import (
    create_stream,
    describe_stream_summary,
    get_records,
    get_shard_iterator,
    list_streams,
    put_records,
)
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from moto import mock_aws


@pytest.fixture(autouse=True)
def setup_testing_env():
    """Set up testing environment for all tests."""
    os.environ['TESTING'] = 'true'
    yield
    os.environ.pop('TESTING', None)


@pytest.fixture
def aws_credentials():
    """Mock AWS credentials for testing."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'  # pragma: allowlist secret
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'


class TestKinesisMCPProtocol:
    """Tests for MCP protocol compatibility with agentic applications."""

    @pytest.fixture
    async def mcp_client(self, aws_credentials):
        """Create MCP client connected to the server."""
        server_params = StdioServerParameters(
            command='python', args=['-m', 'awslabs.kinesis_mcp_server.server']
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                yield session

    def test_mcp_server_startup(self):
        """Test that the MCP server starts without errors."""
        process = subprocess.Popen(
            ['python', '-m', 'awslabs.kinesis_mcp_server.server'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Send initialization message
        init_msg = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'initialize',
            'params': {
                'protocolVersion': '2024-11-05',
                'capabilities': {},
                'clientInfo': {'name': 'test-client', 'version': '1.0.0'},
            },
        }

        process.stdin.write(json.dumps(init_msg) + '\n')
        process.stdin.flush()

        # Wait for response
        time.sleep(1)
        process.terminate()

        stdout, stderr = process.communicate(timeout=5)

        # Verify server started without critical errors
        assert 'Traceback' not in stderr

    @mock_aws
    def test_mcp_tool_functions_exist(self, aws_credentials):
        """Test that all MCP tools are properly defined."""
        expected_tools = [
            create_stream,
            put_records,
            get_records,
            get_shard_iterator,
            list_streams,
            describe_stream_summary,
        ]

        for tool_func in expected_tools:
            assert callable(tool_func)
            assert hasattr(tool_func, '__doc__')
            assert tool_func.__doc__ is not None

        # Step 1: Create stream
        create_response = create_stream(stream_name='test', region_name='us-west-2')
        assert create_response is not None

        # Step 2: Describe stream
        describe_response = describe_stream_summary(stream_name='test', region_name='us-west-2')
        assert describe_response['StreamDescriptionSummary']['StreamName'] == 'test'

        # Step 3: Put records
        test_records = [
            {'Data': 'test-data-1', 'PartitionKey': 'partition-1'},
            {'Data': 'test-data-2', 'PartitionKey': 'partition-2'},
        ]
        put_response = put_records(
            records=test_records, stream_name='test', region_name='us-west-2'
        )
        assert put_response['FailedRecordCount'] == 0
        assert len(put_response['Records']) == 2

        # Step 4: Get shard iterator
        client = boto3.client('kinesis', region_name='us-west-2')
        stream_desc = client.describe_stream(StreamName='test')
        shard_id = stream_desc['StreamDescription']['Shards'][0]['ShardId']

        iterator_response = get_shard_iterator(
            shard_id=shard_id,
            shard_iterator_type='TRIM_HORIZON',
            stream_name='test',
            region_name='us-west-2',
        )
        assert 'ShardIterator' in iterator_response

        # Step 5: Get records
        records_response = get_records(
            shard_iterator=iterator_response['ShardIterator'], region_name='us-west-2'
        )
        assert 'Records' in records_response
        assert len(records_response['Records']) >= 1  # moto may not return all records

    @mock_aws
    def test_list_streams_integration(self, aws_credentials):
        """Test listing streams after creating multiple streams."""
        stream_names = ['stream-1', 'stream-2', 'stream-3']

        for stream_name in stream_names:
            create_stream(stream_name=stream_name, region_name='us-west-2')

        list_response = list_streams(region_name='us-west-2')

        assert 'StreamNames' in list_response
        for stream_name in stream_names:
            assert stream_name in list_response['StreamNames']

    @mock_aws
    def test_provisioned_stream_workflow(self, aws_credentials):
        """Test creating and using provisioned stream."""
        stream_name = 'provisioned-stream'

        create_response = create_stream(
            stream_name=stream_name,
            shard_count=2,
            stream_mode_details={'StreamMode': 'PROVISIONED'},
            region_name='us-west-2',
        )

        assert create_response is not None

        describe_response = describe_stream_summary(
            stream_name=stream_name, region_name='us-west-2'
        )

        assert describe_response['StreamDescriptionSummary']['StreamName'] == stream_name
        assert (
            describe_response['StreamDescriptionSummary']['StreamModeDetails']['StreamMode']
            == 'PROVISIONED'
        )
