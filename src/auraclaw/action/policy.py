from auraclaw.contracts.tools import PolicyDecision, ToolCapability, ToolPermission


class PolicyEngine:
    def __init__(self, *, version: str = "m3-v1") -> None:
        self.version = version

    def evaluate(self, capability: ToolCapability) -> PolicyDecision:
        if capability.permission is ToolPermission.SUGGEST_ONLY:
            return PolicyDecision.DENY
        if capability.permission in {
            ToolPermission.WRITE_WITH_APPROVAL,
            ToolPermission.DESTRUCTIVE_ADMIN,
        }:
            return PolicyDecision.REQUIRE_APPROVAL
        return PolicyDecision.ALLOW
