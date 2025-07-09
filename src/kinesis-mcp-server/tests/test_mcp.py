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

import json
import os
import pytest
import subprocess
import time


@pytest.fixture(autouse=True)
def setup_testing_env():
    """Set up testing environment for all tests."""
    os.environ['TESTING'] = 'true'
    yield
    os.environ.pop('TESTING', None)


def test_mcp_server_startup():
    """Test that the MCP server starts without errors."""
    process = subprocess.Popen(
        ['python', '-m', 'awslabs.kinesis_mcp_server.server'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

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

    time.sleep(1)
    process.terminate()

    stdout, stderr = process.communicate(timeout=5)

    assert 'Traceback' not in stderr


def test_mcp_server_responds_to_initialize():
    """Test that the MCP server responds to initialize message."""
    process = subprocess.Popen(
        ['python', '-m', 'awslabs.kinesis_mcp_server.server'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

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

    # Read response line by line until we get the initialize response
    init_response = None
    for _ in range(10):
        line = process.stdout.readline().strip()
        if not line:
            continue

        try:
            response = json.loads(line)
            if response.get('id') == 1:
                init_response = response
                break
        except json.JSONDecodeError:
            continue

    process.terminate()
    process.communicate(timeout=5)

    # Verify we got an initialize response
    assert init_response is not None
    assert 'result' in init_response
    assert 'serverInfo' in init_response['result']

    assert 'name' in init_response['result']['serverInfo']
    assert 'awslabs.kinesis-mcp-server' == init_response['result']['serverInfo']['name']
