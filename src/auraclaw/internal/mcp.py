from auraclaw.action.mcp import HandsMcpServer
from auraclaw.contracts.mcp import McpJsonRpcRequest, McpJsonRpcResponse, McpTrustedContext


class InProcessMcpTransport:
    def __init__(self, server: HandsMcpServer) -> None:
        self._server = server

    async def send(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse:
        return await self._server.handle(request, trusted_context=trusted_context)
