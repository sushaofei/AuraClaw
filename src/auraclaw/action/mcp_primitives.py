from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

from auraclaw.contracts.mcp import (
    McpPromptDescriptor,
    McpPromptResult,
    McpResourceContent,
    McpResourceDescriptor,
    McpResourceTemplateDescriptor,
    McpTrustedContext,
)

PromptRenderer = Callable[[dict[str, str], McpTrustedContext], McpPromptResult]


@dataclass(frozen=True)
class RegisteredResource:
    descriptor: McpResourceDescriptor
    contents: tuple[McpResourceContent, ...]
    tenant_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RegisteredResourceTemplate:
    descriptor: McpResourceTemplateDescriptor
    tenant_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RegisteredPrompt:
    descriptor: McpPromptDescriptor
    renderer: PromptRenderer
    tenant_ids: tuple[str, ...] = ()


class McpResourceRegistry:
    def __init__(
        self,
        resources: Sequence[RegisteredResource] = (),
        templates: Sequence[RegisteredResourceTemplate] = (),
    ) -> None:
        self._resources: dict[str, RegisteredResource] = {}
        self._templates: dict[str, RegisteredResourceTemplate] = {}
        for resource in resources:
            self.register_resource(resource)
        for template in templates:
            self.register_template(template)

    def register_resource(self, resource: RegisteredResource) -> None:
        uri = resource.descriptor.uri
        if uri in self._resources:
            raise ValueError(f"Resource already registered: {uri}")
        if any(content.uri != uri for content in resource.contents):
            raise ValueError("Resource content URI must match the descriptor URI")
        self._resources[uri] = resource

    def unregister_resource(self, uri: str) -> bool:
        return self._resources.pop(uri, None) is not None

    def register_template(self, template: RegisteredResourceTemplate) -> None:
        uri_template = template.descriptor.uri_template
        if uri_template in self._templates:
            raise ValueError(f"Resource template already registered: {uri_template}")
        self._templates[uri_template] = template

    def discover_resources(self, tenant_id: str) -> list[McpResourceDescriptor]:
        return [
            resource.descriptor
            for resource in sorted(
                self._resources.values(), key=lambda item: item.descriptor.uri
            )
            if _visible_to(resource.tenant_ids, tenant_id)
        ]

    def discover_templates(
        self, tenant_id: str
    ) -> list[McpResourceTemplateDescriptor]:
        return [
            template.descriptor
            for template in sorted(
                self._templates.values(),
                key=lambda item: item.descriptor.uri_template,
            )
            if _visible_to(template.tenant_ids, tenant_id)
        ]

    def read(self, tenant_id: str, uri: str) -> tuple[McpResourceContent, ...]:
        return self.get_resource(tenant_id, uri).contents

    def get_resource(self, tenant_id: str, uri: str) -> RegisteredResource:
        resource = self._resources.get(uri)
        if resource is None or not _visible_to(resource.tenant_ids, tenant_id):
            raise KeyError(f"Resource not found: {uri}")
        return resource


class McpPromptRegistry:
    def __init__(self, prompts: Sequence[RegisteredPrompt] = ()) -> None:
        self._prompts: dict[str, RegisteredPrompt] = {}
        for prompt in prompts:
            self.register(prompt)

    def register(self, prompt: RegisteredPrompt) -> None:
        name = prompt.descriptor.name
        if name in self._prompts:
            raise ValueError(f"Prompt already registered: {name}")
        self._prompts[name] = prompt

    def discover(self, tenant_id: str) -> list[McpPromptDescriptor]:
        return [
            prompt.descriptor
            for prompt in sorted(
                self._prompts.values(), key=lambda item: item.descriptor.name
            )
            if _visible_to(prompt.tenant_ids, tenant_id)
        ]

    def get(
        self,
        tenant_id: str,
        name: str,
        arguments: dict[str, str],
        trusted_context: McpTrustedContext,
    ) -> McpPromptResult:
        prompt = self._prompts.get(name)
        if prompt is None or not _visible_to(prompt.tenant_ids, tenant_id):
            raise KeyError(f"Prompt not found: {name}")
        declared = {argument.name: argument for argument in prompt.descriptor.arguments}
        unknown = sorted(set(arguments).difference(declared))
        if unknown:
            raise ValueError(f"Prompt has unknown arguments: {unknown}")
        missing = sorted(
            name
            for name, argument in declared.items()
            if argument.required and name not in arguments
        )
        if missing:
            raise ValueError(f"Prompt is missing required arguments: {missing}")
        return prompt.renderer(arguments, trusted_context)


def _visible_to(tenant_ids: tuple[str, ...], tenant_id: str) -> bool:
    return not tenant_ids or tenant_id in tenant_ids
