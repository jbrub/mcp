import os
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List
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
    StreamName: str  # Optional
    StreamARN: str  # Optional
    StartingSequenceNumber: str  # Optional - Required if ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER
    Timestamp: datetime  # Optional - Required if ShardIteratorType is AT_TIMESTAMP
