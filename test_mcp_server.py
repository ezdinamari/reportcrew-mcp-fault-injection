import argparse
import asyncio
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


def parse_args():
    parser = argparse.ArgumentParser(
        description="Test the MCP SDK server through the fault-injection proxy."
    )
    parser.add_argument("--config", default="configs/no_faults_sdk.json")
    parser.add_argument("--execution-id", default="sdk-test-001")
    return parser.parse_args()


async def main():
    args = parse_args()

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[
            "proxy.py",
            "--config",
            args.config,
            "--execution-id",
            args.execution_id,
        ],
    )

    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            print("MCP connection initialized through proxy.")

            tools = await session.list_tools()
            print("\nAvailable tools:")
            for tool in tools.tools:
                print(f"- {tool.name}: {tool.description}")

            print("\nCalling get_company_revenue...")
            try:
                result = await session.call_tool(
                    "get_company_revenue",
                    arguments={},
                )

                print("\nTool result:")
                for content in result.content:
                    if hasattr(content, "text"):
                        print(content.text)

                if result.is_error:
                    print("\nOutcome: tool returned an error result.")
                else:
                    print("\nOutcome: tool returned a successful result.")

            except Exception as exc:
                print(f"\nTool call raised {type(exc).__name__}: {exc}")
                print("Outcome: request failed at the MCP client level.")


if __name__ == "__main__":
    asyncio.run(main())
