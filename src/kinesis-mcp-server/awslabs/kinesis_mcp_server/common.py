import os
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Union
from typing_extensions import TypedDict


def handle_exceptions(func: Callable) -> Callable:
    """Decorator to handle exceptions in Kinesis operations.

    Wraps the function in a try-catch block and returns any exceptions in a standardized error format.
    When TESTING environment variable is set, exceptions are re-raised for better testability.

    Args:
        func: The function to wrap

    Returns:
        The wrapped function that handles exceptions
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (ValueError, TypeError):
            # Always re-raise validation errors - these should be visible to users
            raise
        except Exception as e:
            print(f'An error occurred: {e}')
            # Re-raise the exception during testing
            if os.environ.get('TESTING') == 'true':
                raise
            return None

    return wrapper


# TODO: Need to fix this
def is_destructive_action_allowed():
    """Check if destructive actions are allowed based on environment variable.

    Returns:
        tuple: (allowed, error_message) where allowed is a boolean and error_message is None if allowed
    """
    confirm_destruction = os.environ.get('CONFIRM_DESTRUCTIVE_ACTION', 'false').lower()
    if confirm_destruction in ('true', '1', 'yes'):
        return False, {'error': 'Mutation not allowed: CONFIRM_DESTRUCTIVE_ACTION is set to true.'}
    return True, None


class PutRecordsInput(TypedDict, total=False):
    """Input parameters for the put_records operation.

    Attributes:
        Records: List of records to write to the stream
        StreamName: Name of the stream to write to
        StreamARN: ARN of the stream to write to
    """

    Records: List[Dict[str, Any]]  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional


class GetRecordsInput(TypedDict, total=False):
    """Input parameters for the get_records operation.

    Attributes:
        ShardIterator: The shard iterator to use for retrieving records
        Limit: Maximum number of records to retrieve
        StreamARN: ARN of the stream to retrieve records from
    """

    ShardIterator: str  # Required
    Limit: int  # Optional - Default 10000
    StreamARN: str  # Optional


class CreateStreamInput(TypedDict, total=False):
    """Input parameters for the create_stream operation.

    Attributes:
        StreamName: Name of the stream to create
        ShardCount: Number of shards to create
        StreamModeDetails: Details about the stream mode
        Tags: Tags to associate with the stream
    """

    StreamName: str  # Required
    ShardCount: int  # Optional - Default 1
    StreamModeDetails: Dict[str, str]  # Optional - Default ON_DEMAND
    Tags: Dict[str, str]  # Optional


class ListStreamsInput(TypedDict, total=False):
    """Input parameters for the list_streams operation.

    Attributes:
        ExclusiveStartStreamName: Name of the stream to start listing from
        Limit: Maximum number of streams to list
        NextToken: Token for pagination
    """

    ExclusiveStartStreamName: str  # Optional
    Limit: int  # Optional
    NextToken: str  # Optional


class DescribeStreamSummaryInput(TypedDict, total=False):
    """Input parameters for the describe_stream_summary operation.

    Attributes:
        StreamName: Name of the stream to describe
        StreamARN: ARN of the stream to describe
    """

    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional


class GetShardIteratorInput(TypedDict, total=False):
    """Input parameters for the get_shard_iterator operation.

    Attributes:
        ShardId: ID of the shard
        ShardIteratorType: Type of shard iterator
        StreamName: Name of the stream
        StreamARN: ARN of the stream
        StartingSequenceNumber: Starting sequence number (required for AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER)
        Timestamp: Timestamp (required for AT_TIMESTAMP)
    """

    ShardId: str  # Required
    ShardIteratorType: str  # Required - Valid values: AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER | TRIM_HORIZON | LATEST | AT_TIMESTAMP
    StreamName: str  # Optional # Either StreamName or StreamARN required
    StreamARN: str  # Optional # Either StreamName or StreamARN required
    StartingSequenceNumber: str  # Optional - Required if ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER
    Timestamp: Union[datetime, str]  # Optional - Required if ShardIteratorType is AT_TIMESTAMP


class AddTagsToStreamInput(TypedDict, total=False):
    """Input parameters for the add_tags_to_stream operation.

    Attributes:
        Tags: Tags to add to the stream
        StreamName: Name of the stream to add tags to
        StreamARN: ARN of the stream to add tags to
    """

    Tags: Dict[str, str]  # Required - Tags to add
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class DescribeStreamInput(TypedDict, total=False):
    """Input parameters for the describe_stream operation.

    Attributes:
        StreamName: Name of the stream to describe
        StreamARN: ARN of the stream to describe
        Limit: Maximum number of shards to return
        ExclusiveStartShardId: Shard ID to start listing from
    """

    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required
    Limit: int  # Optional - Default 10000
    ExclusiveStartShardId: str  # Optional


class DescribeStreamConsumerInput(TypedDict, total=False):
    """Input parameters for the describe_stream_consumer operation.

    Attributes:
        ConsumerARN: ARN of the consumer to describe
        StreamARN: ARN of the stream the consumer belongs to
        ConsumerName: Name of the consumer to describe
    """

    StreamARN: str  # Required - The stream the consumer belongs to
    ConsumerARN: str  # Optional - Either ConsumerARN or ConsumerName required
    ConsumerName: str  # Optional - Either ConsumerARN or ConsumerName required


class ListStreamConsumersInput(TypedDict, total=False):
    """Input parameters for the list_stream_consumers operation.

    Attributes:
        StreamARN: ARN of the stream to list consumers for
        NextToken: Token for pagination
        StreamCreationTimestamp: Timestamp to filter consumers created after this time
        MaxResults: Maximum number of consumers to return
    """

    StreamARN: str  # Require
    NextToken: str  # Optional
    StreamCreationTimestamp: Union[datetime, str]  # Optional
    MaxResults: int  # Optional - Default 100


class ListTagsForResourceInput(TypedDict, total=False):
    """Input parameters for the list_tags_for_resource operation.

    Attributes:
        ResourceARN: ARN of the resource to list tags for
    """

    ResourceARN: str  # Required - The ARN of the resource to list tags for


class EnableEnhancedMonitoringInput(TypedDict, total=False):
    """Input parameters for the enable_enhanced_monitoring operation.

    Attributes:
        StreamName: Name of the stream to enable monitoring for
        StreamARN: ARN of the stream to enable monitoring for
        ShardLevelMetrics: List of metrics to enable
    """

    ShardLevelMetrics: List[str]  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class GetResourcePolicyInput(TypedDict, total=False):
    """Input parameters for the get_resource_policy operation.

    Attributes:
        ResourceARN: ARN of the resource to get the policy for
    """

    ResourceARN: str  # Required


class IncreaseStreamRetentionPeriodInput(TypedDict, total=False):
    """Input parameters for the increase_stream_retention_period operation.

    Attributes:
        StreamName: Name of the stream to increase retention for
        StreamARN: ARN of the stream to increase retention for
        RetentionPeriodHours: New retention period in hours
    """

    RetentionPeriodHours: int  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class ListShardsInput(TypedDict, total=False):
    """Input parameters for the list_shards operation.

    Attributes:
        ExclusiveStartShardId: Shard ID to start listing from
        StreamName: Name of the stream to list shards for
        StreamARN: ARN of the stream to list shards for
        NextToken: Token for pagination
        MaxResults: Maximum number of shards to return
    """

    ExclusiveStartShardId: str  # Optional
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required
    NextToken: str  # Optional
    MaxResults: int  # Optional - Default 1000


class TagResourceInput(TypedDict, total=False):
    """Input parameters for the tag_resource operation.

    Attributes:
        ResourceARN: ARN of the resource to tag
        Tags: Tags to associate with the resource
    """

    ResourceARN: str  # Required
    Tags: Dict[str, str]  # Required


class ListTagsForStreamInput(TypedDict, total=False):
    """Input parameters for the list_tags_for_stream operation.

    Attributes:
        StreamName: Name of the stream to list tags for
        StreamARN: ARN of the stream to list tags for
        NextToken: Token for pagination
        Limit: Maximum number of tags to return
    """

    ExclusiveStartTagKey: str  # Optional
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required
    Limit: int  # Optional


class PutResourcePolicyInput(TypedDict, total=False):
    """Input parameters for the put_resource_policy operation.

    Attributes:
        ResourceARN: ARN of the resource to attach the policy to
        Policy: JSON policy document as a string
        PolicyName: Name of the policy
    """

    ResourceARN: str  # Required
    Policy: str  # Required - JSON policy document as a string
