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

"""Constants for Kinesis MCP Server."""

# Default Values
DEFAULT_REGION = 'us-west-2'
DEFAULT_SHARD_COUNT = 1
DEFAULT_STREAM_LIMIT = 100
DEFAULT_GET_RECORDS_LIMIT = 10000
DEFAULT_ENCRYPTION_TYPE = 'KMS'
DEFAULT_MAX_RESULTS = 1000
DEFUALT_MAX_RESULTS = 100

# Stream Modes
STREAM_MODE_ON_DEMAND = 'ON_DEMAND'
STREAM_MODE_PROVISIONED = 'PROVISIONED'

# Validation Limits
MIN_STREAM_NAME_LENGTH = 1
MAX_STREAM_NAME_LENGTH = 128
MIN_STREAM_ARN_LENGTH = 1
MAX_STREAM_ARN_LENGTH = 2048
MIN_SHARD_COUNT = 1
MAX_SHARD_COUNT = 10000
MIN_SHARD_ID_LENGTH = 1
MAX_SHARD_ID_LENGTH = 128
MIN_RESULTS_PER_STREAM = 1
MAX_RESULTS_PER_STREAM = 1000
MIN_TAG_KEYS = 1
MAX_TAG_KEYS = 50
MIN_TAG_KEY_LENGTH = 1
MAX_TAG_KEY_LENGTH = 128
MIN_TAG_VALUE_LENGTH = 0
MAX_TAG_VALUE_LENGTH = 256
MAX_TAGS_COUNT = 50
MAX_LENGTH_SHARD_ITERATOR = 512
MAX_LIMIT = 10000

# Valid Values
VALID_SCALING_TYPES = {'UNIFORM_SCALING'}
VALID_SHARD_LEVEL_METRICS = {
    'IncomingBytes',
    'IncomingRecords',
    'OutgoingBytes',
    'OutgoingRecords',
    'WriteProvisionedThroughputExceeded',
    'ReadProvisionedThroughputExceeded',
    'IteratorAgeMilliseconds',
    'All',
}
MIN_SHARD_LEVEL_METRICS = 1
MAX_SHARD_LEVEL_METRICS = 7
