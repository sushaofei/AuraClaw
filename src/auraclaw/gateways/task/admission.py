from auraclaw.contracts.commands import CommandContext


class AllowAllAdmissionController:
    async def admit(self, *, goal: str, context: CommandContext) -> None:
        del goal, context
