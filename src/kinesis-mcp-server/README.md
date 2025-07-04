# AWS Labs Kinesis MCP Server

The official MCP Server for interacting with AWS Kinesis

## Available MCP Tools

### put_records

Writes multiple data records to a Kinesis data stream in a single call.

**Parameters:**
- `stream_name` or `stream_arn` (required): Name or ARN of the stream to describe
- `records`: List of records to write to the stream

**Returns:**
Dictionary containing the sequence number and shard ID of the records

### get_records

Retrieves records from a Kinesis shard.

**Parameters:**
- `shard_iterator`: The shard iterator to use for retrieving records
- `limit` (optional): Maximum number of records to retrieve
- `stream_arn` (optional): ARN of the stream to retrieve records from

**Returns:**
Dictionary containing the retrieved records, next shard iterator, and milliseconds behind latest

### create_stream

Creates a new Kinesis data stream with the specified name and shard count.

**Parameters:**
- `stream_name`: A name to identify the stream
- `shard_count` (optional, default=1): Number of shards to create
- `stream_mode_details` (optional): Details about the stream mode
- `tags` (optional): Tags to associate with the stream

**Returns:**
Dictionary containing the stream name, shard count, stream mode details, tags, and creation status

### list_streams

Lists the Kinesis data streams.

**Parameters:**
- `limit` (optional, default=100): Maximum number of streams to list
- `exclusive_start_stream_name` (optional): Name of the stream to start listing from
- `next_token` (optional): Token for pagination

**Returns:**
Dictionary containing a list of stream names and pagination information

### describe_stream_summary

Describes the stream summary.

**Parameters:**
- `stream_name` (optional): Name of the stream to describe
- `stream_arn` (optional): ARN of the stream to describe

**Note:** Either `stream_name` or `stream_arn` must be provided

**Returns:**
Dictionary containing detailed information about the stream

### get_shard_iterator

Gets an iterator for a specific shard in a Kinesis data stream. The shard iterator is used to read records from the shard.

**Parameters:**
- `shard_id` (required): The ID of the shard to get an iterator for
- `shard_iterator_type` (required): Determines how the shard iterator is used to start reading records from the shard. Valid values: AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST, AT_TIMESTAMP
- `stream_name` or `stream_arn` (optional): Name or ARN of the stream containing the shard
- `starting_sequence_number` (optional): Required when shard_iterator_type is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER
- `timestamp` (optional): Required when shard_iterator_type is AT_TIMESTAMP

**Returns:**
Dictionary containing the shard iterator that can be used with get_records


## Instructions

The official MCP Server for interacting with AWS Kinesis provides a comprehensive set of tools for managing Kinesis resources. Each tool maps directly to Kinesis API operations and supports all relevant parameters.

To use these tools, ensure you have proper AWS credentials configured with appropriate permissions for Kinesis operations. The server will automatically use credentials from environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) or other standard AWS credential sources.

All tools support an optional `region_name` parameter to specify which AWS region to operate in. If not provided, it will use the AWS_REGION environment variable or default to 'us-west-2'.

## Prerequisites

1. Install `uv` from [Astral](https://docs.astral.sh/uv/getting-started/installation/) or the [Github README](https://github.com/astral-sh/uv#installation)
2. Install Python using `uv python install 3.10`
3. Set up AWS credentials with access to AWS services

## Installation

Add the MCP to your favorite agentic tools. e.g. for Amazon Q Developer CLI MCP, `~/.aws/amazonq/mcp.json`:

```
{
  "mcpServers": {
    "awslabs.kinesis-mcp-server": {
      "command": "uvx",
      "args": ["awslabs.kinesis-mcp-server@latest"],
      "env": {
        "DDB-MCP-READONLY": "true",
        "AWS_PROFILE": "default",
        "AWS_REGION": "us-west-2",
        "FASTMCP_LOG_LEVEL": "ERROR"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

or after a successful `docker build -t awslabs/kinesis-mcp-server .`:

```
{
    "mcpServers": {
      "awslabs.kinesis-mcp-server": {
        "command": "docker",
        "args": [
          "run",
          "--rm",
          "--interactive",
          "--env",
          "FASTMCP_LOG_LEVEL=ERROR",
          "awslabs/kinesis-mcp-server:latest"
        ],
        "env": {},
        "disabled": false,
        "autoApprove": []
      }
    }
  }
```
