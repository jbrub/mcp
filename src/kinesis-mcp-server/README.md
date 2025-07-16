# AWS Labs Kinesis MCP Server

The official MCP Server for interacting with AWS Kinesis

## Available MCP Tools

### Read Only Operations

- `describe_limits` - Describes the limits for a Kinesis data stream in the specified region.
- `describe_stream` - Describes the specified stream.
- `describe_stream_consumer` - Describes a Kinesis data stream consumer.
- `describe_stream_summary` - Describes the stream summary.
- `get_records` - Retrieves records from a Kinesis shard.
- `get_resource_policy` - Retrieves the resource policy for a Kinesis data stream.
- `get_shard_iterator` - Retrieves a shard iterator for a specified shard.
- `list_tags_for_resource` - Lists the tags associated with a Kinesis data stream.
- `list_shards` - Lists the shards in a Kinesis data stream.
- `list_stream_consumers` - Lists the consumers of a Kinesis data stream.
- `list_streams` - Lists the Kinesis data streams.


### Non Read Only Operations (requires `KINESIS-READONLY` flag to be set to `false`)

- `create_stream` - Creates a new Kinesis data stream with the specified name and shard count.
- `decrease_stream_retention_period` - Decreases the retention period of a Kinesis data stream.
- `delete_resource_policy` - Deletes the resource policy for a Kinesis data stream.
- `delete_stream` - Deletes a Kinesis data stream.
- `deregister_stream_consumer` - Deregisters a consumer from a Kinesis data stream.
- `disable_enhanced_monitoring` - Disables enhanced monitoring for a Kinesis data stream.
- `enable_enhanced_monitoring` - Enables enhanced monitoring for a Kinesis data stream.
- `increase_stream_retention_period` - Increases the retention period of a Kinesis data stream.
- `merge_shards` - Merges two adjacent shards in a Kinesis data stream.
- `put_record` - Writes a single data record into a Kinesis data stream.
- `put_records` - Writes multiple data records to a Kinesis data stream in a single call.
- `put_resource_policy` - Attaches a resource policy to a Kinesis data stream.
- `register_stream_consumer` - Registers a consumer with a Kinesis data stream.
- `split_shard` - Splits a shard into two shards in a Kinesis data stream.
- `start_stream_encryption` - Starts encryption for a Kinesis data stream.
- `stop_stream_encryption` - Stops encryption for a Kinesis data stream.
- `update_shard_count` - Updates the shard count of a Kinesis data stream.
- `update_stream_mode` - Updates the mode of a Kinesis data stream.


## Instructions

The official MCP Server for interacting with AWS Kinesis provides a comprehensive set of tools for managing Kinesis resources. Each tool maps directly to Kinesis API operations and supports all relevant parameters.

To use these tools, ensure you have proper AWS credentials configured with appropriate permissions for Kinesis operations. The server will automatically use credentials from environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) or other standard AWS credential sources.

All tools support an optional `region_name` parameter to specify which AWS region to operate in. If not provided, it will use the AWS_REGION environment variable or default to 'us-west-2'.

**IMPORTANT**: If you want to have access to non read only tools, you must set the flag in `KINESIS-READONLY` to `false`. The default setup will have this flag set to `true`.

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
        "KINESIS-READONLY": "true",
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
          "KINESIS-READONLY=true",
          "awslabs/kinesis-mcp-server:latest"
        ],
        "env": {},
        "disabled": false,
        "autoApprove": []
      }
    }
  }
```

## Quick Installation

Run the installation script:
```bash
curl -sSL https://raw.githubusercontent.com/jbrub/mcp/main/src/kinesis-mcp-server/install.sh | bash
