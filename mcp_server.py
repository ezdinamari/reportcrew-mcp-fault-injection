
import json

from mcp.server import MCPServer


mcp = MCPServer("ReportCrew Toy Server")


@mcp.tool()
def get_company_revenue() -> str:
    """Retrieve the annual revenue of ExampleCo for FY2025."""

    result = {
        "company": "ExampleCo",
        "year": 2025,
        "currency": "USD",
        "revenue": 120
    }

    return json.dumps(result)


if __name__ == "__main__":
    mcp.run()
