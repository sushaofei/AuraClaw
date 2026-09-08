from importlib.metadata import version

import anyio
import anyio.lowlevel
from mcp import ClientSession, types
from mcp.shared.message import SessionMessage
from mcp.shared.version import SUPPORTED_PROTOCOL_VERSIONS


async def check(protocol):
    server_write, client_read = anyio.create_memory_object_stream(10)
    client_write, server_read = anyio.create_memory_object_stream(10)
    observed = []

    async def server():
        async for message in server_read:
            value = message.message.root
            observed.append(value.method)
            if value.method == "initialize":
                await server_write.send(
                    SessionMessage(
                        types.JSONRPCMessage(
                            types.JSONRPCResponse(
                                jsonrpc="2.0",
                                id=value.id,
                                result={
                                    "protocolVersion": protocol,
                                    "capabilities": {"tools": {}},
                                    "serverInfo": {"name": "fixture", "version": "1"},
                                },
                            )
                        )
                    )
                )

    result = "accepted"
    async with anyio.create_task_group() as group:
        group.start_soon(server)
        async with ClientSession(client_read, client_write) as session:
            try:
                await session.initialize()
                await anyio.lowlevel.checkpoint()
            except RuntimeError:
                result = "unsupported"
        group.cancel_scope.cancel()
    return {"protocol": protocol, "result": result, "methods": observed}


async def main():
    print({"mcp_version": version("mcp"), "supported": SUPPORTED_PROTOCOL_VERSIONS})
    for protocol in ("2025-06-18", "2025-11-25", "2026-07-28"):
        print(await check(protocol))


anyio.run(main)
