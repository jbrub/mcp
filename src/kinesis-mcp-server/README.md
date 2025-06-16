# AWS Labs Kinesis MCP Server

The official MCP Server for interacting with AWS Kinesis

## Available MCP Tools

### put_records

Writes multiple data records to a Kinesis data stream in a single call.

**Parameters:**
- `stream_name`: The name of the stream to write to
- `records`: List of records to write to the stream
- `stream_arn` (optional): ARN of the stream to write to

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
