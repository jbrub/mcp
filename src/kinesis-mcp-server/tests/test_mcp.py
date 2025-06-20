import asyncio
import os
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


# Load environment variables from .env file
load_dotenv()

# Read AWS credentials from environment
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION', 'us-west-2')

if not aws_access_key or not aws_secret_key:
    print('ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set')
    exit(1)


async def test_mcp():
    """Test MCP server integration with AWS Kinesis.

    This test validates:
    - MCP server initialization and connection
    - Tool discovery and listing
    - Kinesis list_streams tool execution

    Requires AWS credentials to be set in environment variables.
    """
    # Build env dict with guaranteed string values
    env_vars = {
        'AWS_ACCESS_KEY_ID': str(aws_access_key),
        'AWS_SECRET_ACCESS_KEY': str(aws_secret_key),
        'AWS_REGION': str(aws_region),
    }

    server_params = StdioServerParameters(
        command='python', args=['-m', 'awslabs.kinesis_mcp_server.server'], env=env_vars
    )

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                print('PASS: Initialize')

                # List tools
                tools = await session.list_tools()
                tool_names = [t.name for t in tools.tools]
                print(f'PASS: Tools found: {tool_names}')

                # Call list_streams
                result = await session.call_tool('list_streams', {'region_name': aws_region})
                print('PASS: Tool execution')

                # Print formatted result
                import json

                response_text = result.content[0].text
                parsed_json = json.loads(response_text)
                print(json.dumps(parsed_json, indent=2))

    except Exception as e:
        import traceback

        print(f'FAIL: {e}')
        print(f'Exception type: {type(e).__name__}')
        print('Full traceback:')
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(test_mcp())
