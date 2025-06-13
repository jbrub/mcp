from functools import wraps
from typing import Any, Callable, Dict, List
from typing_extensions import TypedDict


def handle_exceptions(func: Callable) -> Callable:
    """Decorator to handle exceptions in Kinesis operations.

    Wraps the function in a try-catch block and returns any exceptions in a standardized error format.

    Args:
        func: The function to wrap

    Returns:
        The wrapped function that handles exceptions
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f'An error occurred: {e}')
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
    StreamName: str  # Optional
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
    StreamModeDetails: Dict[str, str]  # Optional
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

    StreamName: str  # Optional
    StreamARN: str  # Optional
