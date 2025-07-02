import asyncio
import os
from datetime import datetime
from functools import wraps
from typing import Any, Dict, List, Union
from typing_extensions import TypedDict


def handle_exceptions(func):
    """Decorator to handle exceptions for both sync and async functions."""
    if asyncio.iscoroutinefunction(func):
        # Async version of the wrapper
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except (ValueError, TypeError):
                # Always re-raise validation errors
                raise
            except Exception as e:
                print(f'An error occurred: {e}')
                # Re-raise the exception during testing
                if os.environ.get('TESTING') == 'true':
                    raise
                return None

        return async_wrapper
    else:

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (ValueError, TypeError):
                # Always re-raise validation errors
                raise
            except Exception as e:
                print(f'An error occurred: {e}')
                # Re-raise the exception during testing
                if os.environ.get('TESTING') == 'true':
                    raise
                return None

        return wrapper


def confirm_destruction(func):
    """Decorator to block mutations if DESTRUCTION-CHECK is set to true."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        allow_destruction = os.environ.get('CONFIRM-DESTRUCTION', '').lower()
        if allow_destruction in ('false', 'no'):  # treat these as true
            return {
                'message': """
                    Deletion blocked: Safety mechanism activated

                    This operation was blocked by a safety mechanism designed to prevent accidental deletion of important resources.

                    To proceed with deletion, you have the following options:

                    1. Set the CONFIRM_DESTRUCTIVE_ACTION environment variable to 'false'
                    2. Use the AWS CLI directly: aws kinesis delete-stream --stream-name <name> --enforce-consumer-deletion
                    3. Use the AWS Console to delete the resource through the web interface

                    WARNING: Deletion will permanently remove all data in this resource.
                """
            }
        return func(*args, **kwargs)

    return wrapper


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

    StreamARN: str  # Optional
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


# =========================================================
# Eleveated permissions tools
# =========================================================


class DeleteStreamInput(TypedDict, total=False):
    """Input parameters for the delete_stream operation.

    Attributes:
        StreamName: Name of the stream to delete
        StreamARN: ARN of the stream to delete
    """

    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required
    EnforceConsumerDeletion: bool  # Optional


class DecreaseStreamRetentionPeriodInput(TypedDict, total=False):
    """Input parameters for the decrease_stream_retention_period operation.

    Attributes:
        StreamName: Name of the stream to decrease retention for
        StreamARN: ARN of the stream to decrease retention for
        RetentionPeriodHours: New retention period in hours
    """

    RetentionPeriodHours: int  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class DeleteResourcePolicyInput(TypedDict, total=False):
    """Input parameters for the delete_resource_policy operation.

    Attributes:
        ResourceARN: ARN of the resource to delete the policy from
    """

    ResourceARN: str  # Required


class DeregisterStreamConsumerInput(TypedDict, total=False):
    """Input parameters for the deregister_stream_consumer operation.

    Attributes:
        ConsumerARN: ARN of the consumer to deregister
        StreamARN: ARN of the stream the consumer belongs to
        ConsumerName: Name of the consumer to deregister
    """

    StreamARN: str  # Optional
    ConsumerARN: str  # Optional - Either ConsumerARN or ConsumerName required
    ConsumerName: str  # Optional - Either ConsumerARN or ConsumerName required


class DisableEnhancedMonitoringInput(TypedDict, total=False):
    """Input parameters for the disable_enhanced_monitoring operation.

    Attributes:
        StreamName: Name of the stream to disable monitoring for
        StreamARN: ARN of the stream to disable monitoring for
        ShardLevelMetrics: List of metrics to disable
    """

    ShardLevelMetrics: List[str]  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class MergeShardsInput(TypedDict, total=False):
    """Input parameters for the merge_shards operation.

    Attributes:
        StreamName: Name of the stream to merge shards in
        StreamARN: ARN of the stream to merge shards in
        ShardToMerge: ID of the shard to merge
        AdjacentShardToMerge: ID of the adjacent shard to merge with
    """

    ShardToMerge: str  # Required
    AdjacentShardToMerge: str  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class RemoveTagsFromStreamInput(TypedDict, total=False):
    """Input parameters for the remove_tags_from_stream operation.

    Attributes:
        StreamName: Name of the stream to remove tags from
        StreamARN: ARN of the stream to remove tags from
        TagKeys: List of tag keys to remove
    """

    TagKeys: List[str]  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class SplitShardInput(TypedDict, total=False):
    """Input parameters for the split_shard operation.

    Attributes:
        StreamName: Name of the stream to split shards in
        StreamARN: ARN of the stream to split shards in
        ShardToSplit: ID of the shard to split
        NewStartingHashKey: New starting hash key for the new shard
    """

    ShardToSplit: str  # Required
    NewStartingHashKey: str  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class StartStreamEncryptionInput(TypedDict, total=False):
    """Input parameters for the start_stream_encryption operation.

    Attributes:
        StreamName: Name of the stream to encrypt
        StreamARN: ARN of the stream to encrypt
        EncryptionType: Type of encryption to use (e.g., KMS)
        KeyId: ID of the KMS key to use for encryption
    """

    EncryptionType: str  # Required
    KeyId: str  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class StopStreamEncryptionInput(TypedDict, total=False):
    """Input parameters for the stop_stream_encryption operation.

    Attributes:
        StreamName: Name of the stream to stop encryption for
        StreamARN: ARN of the stream to stop encryption for
        EncryptionType: Type of encryption to stop (e.g., KMS)
    """

    EncryptionType: str  # Required
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class UntagResourceInput(TypedDict, total=False):
    """Input parameters for the untag_resource operation.

    Attributes:
        ResourceARN: ARN of the resource to remove tags from
        TagKeys: List of tag keys to remove
    """

    ResourceARN: str  # Required
    TagKeys: List[str]  # Required


class UpdateShardCountInput(TypedDict, total=False):
    """Input parameters for the update_shard_count operation.

    Attributes:
        StreamName: Name of the stream to update shard count for
        StreamARN: ARN of the stream to update shard count for
        TargetShardCount: Desired number of shards
        ScalingType: Type of scaling (e.g., UNIFORM_SCALING)
    """

    TargetShardCount: int  # Required
    ScalingType: str  # Required - Valid values: UNIFORM_SCALING | STANDARD_SCALING
    StreamName: str  # Optional - Either StreamName or StreamARN required
    StreamARN: str  # Optional - Either StreamName or StreamARN required


class UpdateStreamModeInput(TypedDict, total=False):
    """Input parameters for the update_stream_mode operation.

    Attributes:
        StreamName: Name of the stream to update mode for
        StreamARN: ARN of the stream to update mode for
        StreamModeDetails: Details about the new stream mode
    """

    StreamModeDetails: Dict[str, str]  # Required
    StreamARN: str  # Required
