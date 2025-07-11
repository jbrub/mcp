import asyncio
import os
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Union
from typing_extensions import TypedDict


def handle_exceptions(func: Callable) -> Callable:
    """Decorator to handle exceptions for both sync and async functions."""
    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except (ValueError, TypeError):
                raise
            except Exception as e:
                """Error messages are formatted and written this way to allow for agentic applications to
                parse and handle errors programmatically. The structured format with error type prefixes
                enables agents to make intelligent decisions based on the specific error encountered."""

                error_message = str(e)
                if 'ResourceNotFoundException' in error_message:
                    return {'error': f'Resource not found: {error_message}'}
                elif 'ValidationException' in error_message:
                    return {'error': f'Validation error: {error_message}'}
                elif 'ResourceInUseException' in error_message:
                    return {'error': f'Resource in use: {error_message}'}
                elif 'LimitExceededException' in error_message:
                    return {'error': f'Limit exceeded: {error_message}'}
                else:
                    print(f'An error occurred: {e}')
                    return {'error': f'An error occurred: {error_message}'}

        return async_wrapper
    else:

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ValueError, TypeError):
                raise
            except Exception as e:
                """Error messages are formatted and written this way to allow for agentic applications to
                parse and handle errors programmatically. The structured format with error type prefixes
                enables agents to make intelligent decisions based on the specific error encountered."""

                error_message = str(e)
                if 'ResourceNotFoundException' in error_message:
                    return {'error': f'Resource not found: {error_message}'}
                elif 'ValidationException' in error_message:
                    return {'error': f'Validation error: {error_message}'}
                elif 'ResourceInUseException' in error_message:
                    return {'error': f'Resource in use: {error_message}'}
                elif 'LimitExceededException' in error_message:
                    return {'error': f'Limit exceeded: {error_message}'}
                else:
                    print(f'An error occurred: {e}')
                    return {'error': f'An error occurred: {error_message}'}

        return wrapper


def mutation_check(func):
    """Decorator to block mutations if KINESIS-READONLY is set to false."""

    @wraps(func)
    async def wrapper(*args, **kwargs):
        read_only = os.environ.get('KINESIS-READONLY', '').lower()
        if read_only not in ('false', 'no'):
            """This return message needs to be specifically worded as "Operation Blocked." If it is not worded
            this way, there is a chance that the agent will disagree and create its own error message. Read developer
            documentation for more information."""
            return {
                'error': """
                    Operation blocked: Elevated permissions required

                    This operation requires elevated permissions to modify Kinesis resources.

                    To proceed with this operation, you have the following options:

                    1. Set the KINESIS-READONLY environment variable to 'false'
                    2. Use the AWS CLI directly with appropriate credentials
                    3. Use the AWS Console with appropriate permissions

                    Note: This is a safety mechanism to prevent unintended modifications to important resources.
                """
            }
        return await func(*args, **kwargs)

    return wrapper


class PutRecordsInput(TypedDict, total=False):
    """Input parameters for the put_records operation.

    Attributes:
        Records: List of records to write to the stream (Required)
        StreamName: Name of the stream to write to (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to write to (Optional)
    """

    Records: List[Dict[str, Any]]
    StreamName: str
    StreamARN: str


class GetRecordsInput(TypedDict, total=False):
    """Input parameters for the get_records operation.

    Attributes:
        ShardIterator: The shard iterator to use for retrieving records (Required)
        Limit: Maximum number of records to retrieve (Optional - Default 10000)
        StreamARN: ARN of the stream to retrieve records from (Optional)
    """

    ShardIterator: str
    Limit: int
    StreamARN: str


class CreateStreamInput(TypedDict, total=False):
    """Input parameters for the create_stream operation.

    Attributes:
        StreamName: Name of the stream to create (Required)
        ShardCount: Number of shards to create (Optional - Default 1)
        StreamModeDetails: Details about the stream mode (Optional - Default ON_DEMAND)
        Tags: Tags to associate with the stream (Optional)
    """

    StreamName: str
    ShardCount: int
    StreamModeDetails: Dict[str, str]
    Tags: Dict[str, str]


class ListStreamsInput(TypedDict, total=False):
    """Input parameters for the list_streams operation.

    Attributes:
        ExclusiveStartStreamName: Name of the stream to start listing from (Optional)
        Limit: Maximum number of streams to list (Optional)
        NextToken: Token for pagination (Optional)
    """

    ExclusiveStartStreamName: str
    Limit: int
    NextToken: str


class DescribeStreamSummaryInput(TypedDict, total=False):
    """Input parameters for the describe_stream_summary operation.

    Attributes:
        StreamName: Name of the stream to describe (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to describe (Optional)
    """

    StreamName: str
    StreamARN: str


class GetShardIteratorInput(TypedDict, total=False):
    """Input parameters for the get_shard_iterator operation.

    Attributes:
        ShardId: ID of the shard (Required)
        ShardIteratorType: Type of shard iterator (Required - Valid values: AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER | TRIM_HORIZON | LATEST | AT_TIMESTAMP)
        StreamName: Name of the stream (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream (Optional - Either StreamName or StreamARN required)
        StartingSequenceNumber: Starting sequence number (Optional - Required if ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER)
        Timestamp: Timestamp (Optional - Required if ShardIteratorType is AT_TIMESTAMP)
    """

    ShardId: str
    ShardIteratorType: str
    StreamName: str
    StreamARN: str
    StartingSequenceNumber: str
    Timestamp: Union[datetime, str]


class AddTagsToStreamInput(TypedDict, total=False):
    """Input parameters for the add_tags_to_stream operation.

    Attributes:
        Tags: Tags to add to the stream (Required)
        StreamName: Name of the stream to add tags to (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to add tags to (Optional - Either StreamName or StreamARN required)
    """

    Tags: Dict[str, str]
    StreamName: str
    StreamARN: str


class DescribeStreamInput(TypedDict, total=False):
    """Input parameters for the describe_stream operation.

    Attributes:
        StreamName: Name of the stream to describe (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to describe (Optional - Either StreamName or StreamARN required)
        Limit: Maximum number of shards to return (Optional - Default 10000)
        ExclusiveStartShardId: Shard ID to start listing from (Optional)
    """

    StreamName: str
    StreamARN: str
    Limit: int
    ExclusiveStartShardId: str


class DescribeStreamConsumerInput(TypedDict, total=False):
    """Input parameters for the describe_stream_consumer operation.

    Attributes:
        StreamARN: ARN of the stream the consumer belongs to (Optional)
        ConsumerARN: ARN of the consumer to describe (Optional - Either ConsumerARN or ConsumerName required)
        ConsumerName: Name of the consumer to describe (Optional - Either ConsumerARN or ConsumerName required)
    """

    StreamARN: str
    ConsumerARN: str
    ConsumerName: str


class ListStreamConsumersInput(TypedDict, total=False):
    """Input parameters for the list_stream_consumers operation.

    Attributes:
        StreamARN: ARN of the stream to list consumers for (Required)
        NextToken: Token for pagination (Optional)
        StreamCreationTimestamp: Timestamp to filter consumers created after this time (Optional)
        MaxResults: Maximum number of consumers to return (Optional - Default 100)
    """

    StreamARN: str
    NextToken: str
    StreamCreationTimestamp: Union[datetime, str]
    MaxResults: int


class ListTagsForResourceInput(TypedDict, total=False):
    """Input parameters for the list_tags_for_resource operation.

    Attributes:
        ResourceARN: ARN of the resource to list tags for (Required)
    """

    ResourceARN: str


class EnableEnhancedMonitoringInput(TypedDict, total=False):
    """Input parameters for the enable_enhanced_monitoring operation.

    Attributes:
        ShardLevelMetrics: List of metrics to enable (Required)
        StreamName: Name of the stream to enable monitoring for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to enable monitoring for (Optional - Either StreamName or StreamARN required)
    """

    ShardLevelMetrics: List[str]
    StreamName: str
    StreamARN: str


class GetResourcePolicyInput(TypedDict, total=False):
    """Input parameters for the get_resource_policy operation.

    Attributes:
        ResourceARN: ARN of the resource to get the policy for (Required)
    """

    ResourceARN: str


class IncreaseStreamRetentionPeriodInput(TypedDict, total=False):
    """Input parameters for the increase_stream_retention_period operation.

    Attributes:
        RetentionPeriodHours: New retention period in hours (Required)
        StreamName: Name of the stream to increase retention for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to increase retention for (Optional - Either StreamName or StreamARN required)
    """

    RetentionPeriodHours: int
    StreamName: str
    StreamARN: str


class ListShardsInput(TypedDict, total=False):
    """Input parameters for the list_shards operation.

    Attributes:
        ExclusiveStartShardId: Shard ID to start listing from (Optional)
        StreamName: Name of the stream to list shards for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to list shards for (Optional - Either StreamName or StreamARN required)
        NextToken: Token for pagination (Optional)
        MaxResults: Maximum number of shards to return (Optional - Default 1000)
    """

    ExclusiveStartShardId: str
    StreamName: str
    StreamARN: str
    NextToken: str
    MaxResults: int


class TagResourceInput(TypedDict, total=False):
    """Input parameters for the tag_resource operation.

    Attributes:
        ResourceARN: ARN of the resource to tag (Required)
        Tags: Tags to associate with the resource (Required)
    """

    ResourceARN: str
    Tags: Dict[str, str]


class ListTagsForStreamInput(TypedDict, total=False):
    """Input parameters for the list_tags_for_stream operation.

    Attributes:
        ExclusiveStartTagKey: Key to start listing from (Optional)
        StreamName: Name of the stream to list tags for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to list tags for (Optional - Either StreamName or StreamARN required)
        Limit: Maximum number of tags to return (Optional)
    """

    ExclusiveStartTagKey: str
    StreamName: str
    StreamARN: str
    Limit: int


class PutResourcePolicyInput(TypedDict, total=False):
    """Input parameters for the put_resource_policy operation.

    Attributes:
        ResourceARN: ARN of the resource to attach the policy to (Required)
        Policy: JSON policy document as a string (Required)
    """

    ResourceARN: str
    Policy: str


class DeleteStreamInput(TypedDict, total=False):
    """Input parameters for the delete_stream operation.

    Attributes:
        StreamName: Name of the stream to delete (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to delete (Optional - Either StreamName or StreamARN required)
        EnforceConsumerDeletion: Whether to enforce consumer deletion (Optional)
    """

    StreamName: str
    StreamARN: str
    EnforceConsumerDeletion: bool


class DecreaseStreamRetentionPeriodInput(TypedDict, total=False):
    """Input parameters for the decrease_stream_retention_period operation.

    Attributes:
        RetentionPeriodHours: New retention period in hours (Required)
        StreamName: Name of the stream to decrease retention for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to decrease retention for (Optional - Either StreamName or StreamARN required)
    """

    RetentionPeriodHours: int
    StreamName: str
    StreamARN: str


class DeleteResourcePolicyInput(TypedDict, total=False):
    """Input parameters for the delete_resource_policy operation.

    Attributes:
        ResourceARN: ARN of the resource to delete the policy from (Required)
    """

    ResourceARN: str


class DeregisterStreamConsumerInput(TypedDict, total=False):
    """Input parameters for the deregister_stream_consumer operation.

    Attributes:
        StreamARN: ARN of the stream the consumer belongs to (Optional)
        ConsumerARN: ARN of the consumer to deregister (Optional - Either ConsumerARN or ConsumerName required)
        ConsumerName: Name of the consumer to deregister (Optional - Either ConsumerARN or ConsumerName required)
    """

    StreamARN: str
    ConsumerARN: str
    ConsumerName: str


class DisableEnhancedMonitoringInput(TypedDict, total=False):
    """Input parameters for the disable_enhanced_monitoring operation.

    Attributes:
        ShardLevelMetrics: List of metrics to disable (Required)
        StreamName: Name of the stream to disable monitoring for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to disable monitoring for (Optional - Either StreamName or StreamARN required)
    """

    ShardLevelMetrics: List[str]
    StreamName: str
    StreamARN: str


class MergeShardsInput(TypedDict, total=False):
    """Input parameters for the merge_shards operation.

    Attributes:
        ShardToMerge: ID of the shard to merge (Required)
        AdjacentShardToMerge: ID of the adjacent shard to merge with (Required)
        StreamName: Name of the stream to merge shards in (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to merge shards in (Optional - Either StreamName or StreamARN required)
    """

    ShardToMerge: str
    AdjacentShardToMerge: str
    StreamName: str
    StreamARN: str


class RemoveTagsFromStreamInput(TypedDict, total=False):
    """Input parameters for the remove_tags_from_stream operation.

    Attributes:
        TagKeys: List of tag keys to remove (Required)
        StreamName: Name of the stream to remove tags from (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to remove tags from (Optional - Either StreamName or StreamARN required)
    """

    TagKeys: List[str]
    StreamName: str
    StreamARN: str


class SplitShardInput(TypedDict, total=False):
    """Input parameters for the split_shard operation.

    Attributes:
        ShardToSplit: ID of the shard to split (Required)
        NewStartingHashKey: New starting hash key for the new shard (Required)
        StreamName: Name of the stream to split shards in (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to split shards in (Optional - Either StreamName or StreamARN required)
    """

    ShardToSplit: str
    NewStartingHashKey: str
    StreamName: str
    StreamARN: str


class StartStreamEncryptionInput(TypedDict, total=False):
    """Input parameters for the start_stream_encryption operation.

    Attributes:
        EncryptionType: Type of encryption to use (Required - Default: KMS)
        KeyId: ID of the KMS key to use for encryption (Required)
        StreamName: Name of the stream to encrypt (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to encrypt (Optional - Either StreamName or StreamARN required)
    """

    EncryptionType: str
    KeyId: str
    StreamName: str
    StreamARN: str


class StopStreamEncryptionInput(TypedDict, total=False):
    """Input parameters for the stop_stream_encryption operation.

    Attributes:
        EncryptionType: Type of encryption to stop (Required - Default: KMS)
        StreamName: Name of the stream to stop encryption for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to stop encryption for (Optional - Either StreamName or StreamARN required)
    """

    EncryptionType: str
    StreamName: str
    StreamARN: str


class UntagResourceInput(TypedDict, total=False):
    """Input parameters for the untag_resource operation.

    Attributes:
        ResourceARN: ARN of the resource to remove tags from (Required)
        TagKeys: List of tag keys to remove (Required)
    """

    ResourceARN: str
    TagKeys: List[str]


class UpdateShardCountInput(TypedDict, total=False):
    """Input parameters for the update_shard_count operation.

    Attributes:
        TargetShardCount: Desired number of shards (Required)
        ScalingType: Type of scaling (Required - Valid values: UNIFORM_SCALING)
        StreamName: Name of the stream to update shard count for (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to update shard count for (Optional - Either StreamName or StreamARN required)
    """

    TargetShardCount: int
    ScalingType: str
    StreamName: str
    StreamARN: str


class UpdateStreamModeInput(TypedDict, total=False):
    """Input parameters for the update_stream_mode operation.

    Attributes:
        StreamModeDetails: Details about the new stream mode (Required)
        StreamARN: ARN of the stream to update mode for (Required)
    """

    StreamModeDetails: Dict[str, str]
    StreamARN: str


class PutRecordInput(TypedDict, total=False):
    """Input parameters for the put_record operation.

    Attributes:
        Data: Data to put in the record (Required)
        PartitionKey: Partition key for the record (Required)
        StreamName: Name of the stream to put the record in (Optional - Either StreamName or StreamARN required)
        StreamARN: ARN of the stream to put the record in (Optional - Either StreamName or StreamARN required)
        ExplicitHashKey: Explicit hash key for the record (Optional)
        SequenceNumberForOrdering: Sequence number for the record (Optional)
    """

    Data: bytes
    PartitionKey: str
    StreamName: str
    StreamARN: str
    ExplicitHashKey: str
    SequenceNumberForOrdering: str


class RegisterStreamConsumerInput(TypedDict, total=False):
    """Input parameters for the register_stream_consumer operation.

    Attributes:
        ConsumerName: Name of the consumer to register (Required)
        StreamARN: ARN of the stream to register the consumer for (Required)
    """

    ConsumerName: str
    StreamARN: str
    Tags: Dict[str, str]
