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

# =============================================================================
# Default Values
# =============================================================================

DEFAULT_REGION = 'us-west-2'
DEFAULT_SHARD_COUNT = 1
DEFAULT_STREAM_LIMIT = 100
DEFAULT_GET_RECORDS_LIMIT = 10000
DEFAULT_ENCRYPTION_TYPE = 'KMS'
DEFUALT_MAX_RESULTS = 100
DEFAULT_MAX_RESULTS = 1000

# =============================================================================
# Stream Configuration
# =============================================================================

# Stream Modes
STREAM_MODE_ON_DEMAND = 'ON_DEMAND'
STREAM_MODE_PROVISIONED = 'PROVISIONED'
DEFAULT_STREAM_MODE = STREAM_MODE_ON_DEMAND

# Stream Status
STREAM_STATUS_CREATING = 'CREATING'

# =============================================================================
# Validation Limits - General
# =============================================================================

# Stream Name/ARN Limits
MAX_STREAM_NAME_LENGTH = 128
MIN_STREAM_NAME_LENGTH = 1
MAX_STREAM_ARN_LENGTH = 2048
MIN_STREAM_ARN_LENGTH = 1

# General Limits
MAX_LIMIT = 10000
MIN_LIMIT = 1

# Shard Configuration
MAX_SHARDS_PER_STREAM = 500
MIN_SHARDS_PER_STREAM = 1
MAX_SHARD_ID_LENGTH = 128
MIN_SHARD_ID_LENGTH = 1

# ListStreamConsumers Limits
MAX_RESULTS_PER_STREAM = 1000
MIN_RESULTS_PER_STREAM = 1

MIN_TAG_KEYS = 1
MAX_TAG_KEYS = 50

# =============================================================================
# Validation Limits - Records
# =============================================================================

# Record Limits
MAX_RECORDS = 500
MIN_RECORDS = 1

# Shard Iterator
MAX_LENGTH_SHARD_ITERATOR = 512
MIN_LENGTH_SHARD_ITERATOR = 1
VALID_SHARD_ITERATOR_TYPES = {
    'AT_SEQUENCE_NUMBER',
    'AFTER_SEQUENCE_NUMBER',
    'TRIM_HORIZON',
    'LATEST',
    'AT_TIMESTAMP',
}

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

MIN_MAX_RESULTS = 1
MAX_MAX_RESULTS = 10000

VALID_SCALING_TYPES = {
    'UNIFORM_SCALING',
    'STANDARD_SCALING',
}

MIN_SHARD_COUNT = 1
MAX_SHARD_COUNT = 10000

# =============================================================================
# Validation Limits - Tags
# =============================================================================

MAX_TAGS_COUNT = 50
MAX_TAG_ENTRIES = 200
MAX_TAG_KEY_LENGTH = 128
MIN_TAG_KEY_LENGTH = 1
MAX_TAG_VALUE_LENGTH = 256
MIN_TAG_VALUE_LENGTH = 0

# =============================================================================
# Validation Limits - Pagination
# =============================================================================

MAX_EXCLUSIVE_START_STREAM_NAME_LENGTH = 128
MIN_EXCLUSIVE_START_STREAM_NAME_LENGTH = 1
MAX_NEXT_TOKEN_LENGTH = 1048576
MIN_NEXT_TOKEN_LENGTH = 1


# =============================================================================
# Validation Limits - Target Shard Count
# =============================================================================

MIN_TARGET_SHARD_COUNT = 1
MAX_TARGET_SHARD_COUNT = 10000
VALID_SCALING_TYPES = {'UNIFORM_SCALING'}
