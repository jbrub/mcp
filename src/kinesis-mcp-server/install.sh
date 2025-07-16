#!/bin/bash

# AWS Kinesis MCP Server Installation Script

set -e

echo "Installing AWS Kinesis MCP Server..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "uv is not installed. Please install uv first:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Create MCP config directory
MCP_CONFIG_DIR="$HOME/.aws/amazonq"
mkdir -p "$MCP_CONFIG_DIR"

# Check if mcp.json exists
MCP_CONFIG_FILE="$MCP_CONFIG_DIR/mcp.json"

if [ -f "$MCP_CONFIG_FILE" ]; then
    echo "📝 Backing up existing mcp.json..."
    cp "$MCP_CONFIG_FILE" "$MCP_CONFIG_FILE.backup.$(date +%s)"
fi

# Create or update mcp.json
cat > "$MCP_CONFIG_FILE" << 'EOF'
{
  "mcpServers": {
    "awslabs.kinesis-mcp-server": {
      "command": "uvx",
      "args": ["awslabs.kinesis-mcp-server@latest"],
      "env": {
        "KINESIS-READONLY": "true"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
EOF

echo "AWS Kinesis MCP Server installed successfully!"
echo ""
echo "Configuration:"
echo "   - Config file: $MCP_CONFIG_FILE"
echo "   - Mode: Read-only (safe mode)"
echo "   - Default region: us-west-2"
echo ""
echo "To enable write operations, change KINESIS-READONLY to 'false'"
echo "Make sure your AWS credentials are configured"
echo ""
echo "Ready to use with Amazon Q Developer!"
