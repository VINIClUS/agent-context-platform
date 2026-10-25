"""The conformance suite's fixture tools on the shipping MCP server stack (PLATFORM-047). TEST-ONLY.

`create_fixture_server` starts from the shipping `create_mcp_server` (served surface, sorted
listing, list cache hints, the platform's trace-context middleware) and registers the tools the
pinned suite calls by name. `loopback.fixture_app` mounts it with the shipping `build_mcp_mount`,
so every scenario that needs these tools runs through our protocol guard, authentication and
stateless JSON-response transport. The shipping application never registers any of them (a test
asserts that its listing holds exactly the five memory tools), and `src/` never imports this file.

Only tools whose behaviour the platform transport can carry are implemented: content kinds, a
tool error, progress, the client-capability check (-32021), logging without a log level, a
streaming diagnostic, JSON Schema 2020-12 keyword preservation and an `x-mcp-header` parameter.
Prompts, resources, completion, tasks and input requests (elicitation, sampling, roots) are not
served (see `applicability.toml`).
"""

from __future__ import annotations

import warnings
from typing import Annotated, Any, Final

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.shared.exceptions import MCPDeprecationWarning, MCPError
from mcp_types import (
    AudioContent,
    EmbeddedResource,
    ImageContent,
    TextContent,
    TextResourceContents,
)
from pydantic import Field

from agent_context_platform.mcp.server import create_mcp_server

# 1x1 transparent PNG and a 44-byte silent WAV header: tiny, valid media payloads.
PNG_BASE64: Final = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
)
WAV_BASE64: Final = "UklGRiQAAABXQVZFZm10IBAAAAABAAEAQB8AAIA+AAACABAAZGF0YQAAAAA="
JSON_SCHEMA_TOOL: Final = "json_schema_2020_12_tool"
# The exact input schema the pinned scenario `json-schema-2020-12` expects to be preserved.
JSON_SCHEMA_2020_12: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "$defs": {
        "address": {
            "$anchor": "addressDef",
            "type": "object",
            "properties": {"street": {"type": "string"}, "city": {"type": "string"}},
        }
    },
    "properties": {
        "name": {"type": "string"},
        "address": {"$ref": "#/$defs/address"},
        "contactMethod": {"type": "string", "enum": ["phone", "email"]},
        "phone": {"type": "string"},
        "email": {"type": "string"},
    },
    "allOf": [{"anyOf": [{"required": ["phone"]}, {"required": ["email"]}]}],
    "if": {"properties": {"contactMethod": {"const": "phone"}}, "required": ["contactMethod"]},
    "then": {"required": ["phone"]},
    "else": {"required": ["email"]},
    "additionalProperties": False,
}
FIXTURE_TOOLS: Final = frozenset(
    {
        "test_simple_text",
        "test_image_content",
        "test_audio_content",
        "test_embedded_resource",
        "test_multiple_content_types",
        "test_error_handling",
        "test_tool_with_progress",
        "test_missing_capability",
        "test_streaming_elicitation",
        "test_logging_tool",
        "test_custom_headers",
        JSON_SCHEMA_TOOL,
    }
)
MISSING_REQUIRED_CLIENT_CAPABILITY: Final = -32021
# The SDK release whose private tool registry `create_fixture_server` was reviewed against; a
# test fails as soon as the lock moves to another one.
REVIEWED_SDK_VERSION: Final = "2.2.0"


def _text(text: str) -> TextContent:
    return TextContent(type="text", text=text)


def test_simple_text() -> list[Any]:
    """Returns one text block."""
    return [_text("This is a simple text response for testing.")]


def test_image_content() -> list[Any]:
    """Returns one PNG image block."""
    return [ImageContent(type="image", data=PNG_BASE64, mime_type="image/png")]


def test_audio_content() -> list[Any]:
    """Returns one WAV audio block."""
    return [AudioContent(type="audio", data=WAV_BASE64, mime_type="audio/wav")]


def test_embedded_resource() -> list[Any]:
    """Returns one embedded text resource."""
    return [
        EmbeddedResource(
            type="resource",
            resource=TextResourceContents(
                uri="test://embedded-resource",
                mime_type="text/plain",
                text="This is an embedded resource content.",
            ),
        )
    ]


def test_multiple_content_types() -> list[Any]:
    """Returns text, an image and an embedded JSON resource."""
    return [
        _text("Multiple content types test:"),
        ImageContent(type="image", data=PNG_BASE64, mime_type="image/png"),
        EmbeddedResource(
            type="resource",
            resource=TextResourceContents(
                uri="test://mixed-content-resource",
                mime_type="application/json",
                text='{"test":"data","value":123}',
            ),
        ),
    ]


def test_error_handling() -> list[Any]:
    """Always answers a tool error."""
    raise ToolError("This tool intentionally returns an error for testing")


async def test_tool_with_progress(ctx: Context[Any, Any]) -> list[Any]:
    """Reports progress 0, 50 and 100 of 100, then answers."""
    for progress in (0, 50, 100):
        await ctx.report_progress(progress, 100)
    return [_text("Progress reported.")]


def test_missing_capability(ctx: Context[Any, Any]) -> list[Any]:
    """Needs the client's `sampling` capability, and refuses with -32021 without it."""
    capabilities = ctx.client_capabilities
    if capabilities is None or capabilities.sampling is None:
        raise MCPError(
            code=MISSING_REQUIRED_CLIENT_CAPABILITY,
            message="Client did not declare the sampling capability",
            data={"requiredCapabilities": {"sampling": {}}},
        )
    return [_text("The client declared sampling.")]


def test_streaming_elicitation() -> list[Any]:
    """The platform never sends a request on a response stream: it answers with one result."""
    return [_text("No input is required; the answer is complete.")]


async def test_logging_tool(ctx: Context[Any, Any]) -> list[Any]:
    """Logs while it runs; nothing may reach a client that did not ask for a log level."""
    # Logging is deprecated in 2026-07-28 (SEP-2577); the diagnostic is that nothing is sent.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", MCPDeprecationWarning)
        await ctx.info("diagnostic log line")
    return [_text("Logged one line.")]


def test_custom_headers(
    region: Annotated[str, Field(json_schema_extra={"x-mcp-header": "Region"})],
) -> list[Any]:
    """A string parameter mirrored in an `Mcp-Param-Region` header (SEP-2243)."""
    return [_text("Header and body agree.")]


def json_schema_2020_12_tool(
    name: str | None = None,
    address: dict[str, Any] | None = None,
    contactMethod: str | None = None,  # the scenario's property name
    phone: str | None = None,
    email: str | None = None,
) -> list[Any]:
    """Takes the JSON Schema 2020-12 input the scenario expects to be listed verbatim."""
    return [_text("Schema accepted.")]


def create_fixture_server() -> MCPServer[Any]:
    """The shipping server with the suite's fixture tools registered."""
    server = create_mcp_server(advertise_tools=True)
    for function in (
        test_simple_text,
        test_image_content,
        test_audio_content,
        test_embedded_resource,
        test_multiple_content_types,
        test_error_handling,
        test_tool_with_progress,
        test_missing_capability,
        test_streaming_elicitation,
        test_logging_tool,
        test_custom_headers,
        json_schema_2020_12_tool,
    ):
        server.add_tool(function, structured_output=False)
    # The listing must carry the scenario's schema verbatim, not the one derived from the
    # signature. The SDK has no public API for a hand-written input schema: it keeps the derived
    # one on the registered tool, so replace it there (reviewed for REVIEWED_SDK_VERSION only).
    server._tool_manager.get_tool(JSON_SCHEMA_TOOL).parameters = JSON_SCHEMA_2020_12  # type: ignore[union-attr]
    return server
