#!/bin/bash

# AWS Kinesis MCP Server Cross-Platform Installation Script

set -e

echo "Installing AWS Kinesis MCP Server..."

# Detect operating system
detect_os() {
    case "$(uname -s)" in
        Darwin*)    echo "macos" ;;
        Linux*)     echo "linux" ;;
        CYGWIN*|MINGW*|MSYS*) echo "windows" ;;
        *)          echo "unknown" ;;
    esac
}

OS=$(detect_os)
echo "Detected OS: $OS"

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "uv is not installed. Installing uv..."
    case $OS in
        "macos"|"linux")
            curl -LsSf https://astral.sh/uv/install.sh | sh
            ;;
        "windows")
            echo "Please install uv manually from: https://docs.astral.sh/uv/getting-started/installation/"
            exit 1
            ;;
        *)
            echo "Unsupported OS. Please install uv manually."
            exit 1
            ;;
    esac
fi

# Set MCP config directory based on OS
case $OS in
    "macos"|"linux")
        MCP_CONFIG_DIR="$HOME/.aws/amazonq"
        ;;
    "windows")
        MCP_CONFIG_DIR="$USERPROFILE/.aws/amazonq"
        ;;
    *)
        echo "Unsupported operating system: $OS"
        exit 1
        ;;
esac

echo "Using config directory: $MCP_CONFIG_DIR"
mkdir -p "$MCP_CONFIG_DIR"

# Check if mcp.json exists
MCP_CONFIG_FILE="$MCP_CONFIG_DIR/mcp.json"

if [ -f "$MCP_CONFIG_FILE" ]; then
    echo "Backing up existing mcp.json..."
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
echo "   - OS: $OS"
echo "   - Config file: $MCP_CONFIG_FILE"
echo "   - Mode: Read-only (safe mode)"
echo "   - Default region: us-west-2"
echo ""
echo "To enable write operations, change KINESIS-READONLY to 'false'"
echo "Make sure your AWS credentials are configured"
echo ""

# OS-specific next steps
case $OS in
    "macos")
        echo "macOS: Restart Amazon Q Developer to load the new MCP server"
        ;;
    "linux")
        echo "Linux: Restart Amazon Q Developer to load the new MCP server"
        ;;
    "windows")
        echo "Windows: Restart Amazon Q Developer to load the new MCP server"
        ;;
esac

echo ""
echo "Ready to use with Amazon Q Developer!"
