from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace

from auraclaw.contracts.hands import (
    HandsPromptDescriptor,
    HandsPromptResult,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsTrustedContext,
)

PromptRenderer = Callable[[dict[str, str], HandsTrustedContext], HandsPromptResult]


@dataclass(frozen=True)
class RegisteredResource:
    descriptor: HandsResourceDescriptor
    contents: tuple[HandsResourceContent, ...]
    tenant_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RegisteredResourceTemplate:
    descriptor: HandsResourceDescriptor
    tenant_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class RegisteredPrompt:
    descriptor: HandsPromptDescriptor
    renderer: PromptRenderer
    tenant_ids: tuple[str, ...] = ()


class HandsResourceRegistry:
    def __init__(
        self,
        resources: Sequence[RegisteredResource] = (),
        templates: Sequence[RegisteredResourceTemplate] = (),
    ) -> None:
        self._resources: dict[str, list[RegisteredResource]] = {}
        self._templates: dict[str, list[RegisteredResourceTemplate]] = {}
        for resource in resources:
            self.register_resource(resource)
        for template in templates:
            self.register_template(template)

    def register_resource(self, resource: RegisteredResource) -> None:
        uri = resource.descriptor.uri
        if uri is None:
            raise ValueError("Resource descriptor must define a uri")
        if any(content.uri != uri for content in resource.contents):
            raise ValueError("Resource content URI must match the descriptor URI")
        registrations = self._resources.setdefault(uri, [])
        if any(
            _tenant_scopes_overlap(current.tenant_ids, resource.tenant_ids)
            for current in registrations
        ):
            raise ValueError(f"Resource already registered: {uri}")
        registrations.append(resource)

    def unregister_resource(self, uri: str, *, tenant_id: str | None = None) -> bool:
        if tenant_id is None:
            return self._resources.pop(uri, None) is not None
        registrations = self._resources.get(uri)
        if registrations is None:
            return False
        changed = False
        remaining: list[RegisteredResource] = []
        for resource in registrations:
            if tenant_id not in resource.tenant_ids:
                remaining.append(resource)
                continue
            changed = True
            other_tenants = tuple(
                current for current in resource.tenant_ids if current != tenant_id
            )
            if other_tenants:
                remaining.append(replace(resource, tenant_ids=other_tenants))
        if remaining:
            self._resources[uri] = remaining
        else:
            self._resources.pop(uri, None)
        return changed

    def register_template(self, template: RegisteredResourceTemplate) -> None:
        uri_template = template.descriptor.uri_template
        if uri_template is None:
            raise ValueError("Resource template descriptor must define uri_template")
        registrations = self._templates.setdefault(uri_template, [])
        if any(
            _tenant_scopes_overlap(current.tenant_ids, template.tenant_ids)
            for current in registrations
        ):
            raise ValueError(f"Resource template already registered: {uri_template}")
        registrations.append(template)

    def discover_resources(self, tenant_id: str) -> list[HandsResourceDescriptor]:
        return [
            resource.descriptor
            for resource in sorted(
                (
                    resource
                    for registrations in self._resources.values()
                    for resource in registrations
                ),
                key=lambda item: item.descriptor.uri or item.descriptor.name,
            )
            if _visible_to(resource.tenant_ids, tenant_id)
        ]

    def discover_templates(self, tenant_id: str) -> list[HandsResourceDescriptor]:
        return [
            template.descriptor
            for template in sorted(
                (
                    template
                    for registrations in self._templates.values()
                    for template in registrations
                ),
                key=lambda item: item.descriptor.uri_template or item.descriptor.name,
            )
            if _visible_to(template.tenant_ids, tenant_id)
        ]

    def read(self, tenant_id: str, uri: str) -> tuple[HandsResourceContent, ...]:
        return self.get_resource(tenant_id, uri).contents

    def get_resource(self, tenant_id: str, uri: str) -> RegisteredResource:
        resource = next(
            (
                current
                for current in self._resources.get(uri, ())
                if _visible_to(current.tenant_ids, tenant_id)
            ),
            None,
        )
        if resource is None:
            raise KeyError(f"Resource not found: {uri}")
        return resource

    def has_resource(self, tenant_id: str, uri: str) -> bool:
        return any(
            _visible_to(resource.tenant_ids, tenant_id)
            for resource in self._resources.get(uri, ())
        )


class HandsPromptRegistry:
    def __init__(self, prompts: Sequence[RegisteredPrompt] = ()) -> None:
        self._prompts: dict[str, RegisteredPrompt] = {}
        for prompt in prompts:
            self.register(prompt)

    def register(self, prompt: RegisteredPrompt) -> None:
        name = prompt.descriptor.name
        if name in self._prompts:
            raise ValueError(f"Prompt already registered: {name}")
        self._prompts[name] = prompt

    def discover(self, tenant_id: str) -> list[HandsPromptDescriptor]:
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
        trusted_context: HandsTrustedContext,
    ) -> HandsPromptResult:
        prompt = self._prompts.get(name)
        if prompt is None or not _visible_to(prompt.tenant_ids, tenant_id):
            raise KeyError(f"Prompt not found: {name}")
        declared = {argument.name: argument for argument in prompt.descriptor.arguments}
        unknown = sorted(set(arguments).difference(declared))
        if unknown:
            raise ValueError(f"Prompt has unknown arguments: {unknown}")
        missing = sorted(
            argument_name
            for argument_name, argument in declared.items()
            if argument.required and argument_name not in arguments
        )
        if missing:
            raise ValueError(f"Prompt is missing required arguments: {missing}")
        return prompt.renderer(arguments, trusted_context)


def _visible_to(tenant_ids: tuple[str, ...], tenant_id: str) -> bool:
    return not tenant_ids or tenant_id in tenant_ids


def _tenant_scopes_overlap(first: tuple[str, ...], second: tuple[str, ...]) -> bool:
    return not first or not second or not set(first).isdisjoint(second)


# Compatibility aliases while callers migrate off MCP-named registries.
McpResourceRegistry = HandsResourceRegistry
McpPromptRegistry = HandsPromptRegistry
