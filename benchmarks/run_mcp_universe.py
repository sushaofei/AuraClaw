"""Runtime/MCP episode with an isolated Blender sandbox and separate official grader."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import shlex
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from plan_components import checked_checkout
from run_longmemeval import write_private

from auraclaw.composition.evaluation import run_benchmark_episode
from auraclaw.evaluation.components.catalog import COMPONENT_BENCHMARKS
from auraclaw.evaluation.components.mcp_universe import McpUniverseEnvironment
from auraclaw.evaluation.contracts import EvaluationConfig
from auraclaw.infrastructure.clients.model import RemoteModelClient


class BlenderTransport:
    def __init__(self, prefix: list[str], image: str, output: Path) -> None:
        if not image.startswith("sha256:") or len(image) != 71:
            raise ValueError("Require an immutable admitted image ID")
        self.prefix, self.image, self.output = prefix, image, output
        self.name = "auraclaw-mcp-" + uuid4().hex
        self.client: ClientSession | None = None

    def command(self, args: list[str]) -> list[str]:
        # An optional SSH prefix terminates with the verified host; quote its one remote command.
        return [*self.prefix, shlex.join(args)] if self.prefix else args

    async def process(self, args: list[str], data: bytes | None = None) -> bytes:
        proc = await asyncio.create_subprocess_exec(
            *self.command(args),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(data), 180)
        except BaseException:
            proc.kill()
            await proc.wait()
            raise
        if proc.returncode:
            raise RuntimeError("Container operation failed: " + stderr.decode()[-1000:])
        return stdout

    def isolation(self) -> list[str]:
        return [
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--cpus=2",
            "--memory=4g",
            "--pids-limit=256",
            "--tmpfs=/tmp:rw,nosuid,nodev,size=1g,mode=1777",
            "--tmpfs=/workspace:rw,nosuid,nodev,size=512m,mode=1777",
        ]

    def agent_isolation(self, network: str | None, proxy: str | None) -> list[str]:
        flags = self.isolation() + ["--http-proxy=false"]
        if network is None and proxy is None:
            return flags
        endpoint = urlsplit(proxy or "")
        if (
            not network
            or not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", network)
            or endpoint.scheme != "http"
            or not endpoint.hostname
            or not endpoint.port
            or endpoint.username
            or endpoint.password
            or endpoint.query
            or endpoint.fragment
            or endpoint.path not in ("", "/")
        ):
            raise ValueError(
                "Asset access requires an admitted internal network and credential-free proxy"
            )
        # Host admission must prove this network has no direct egress and only
        # permits the domain-allowlisted proxy. Grading always remains offline.
        flags[0] = "--network=" + network
        return flags + [
            "--env=http_proxy=" + proxy,
            "--env=https_proxy=" + proxy,
            "--env=HTTP_PROXY=" + proxy,
            "--env=HTTPS_PROXY=" + proxy,
            "--env=NO_PROXY=127.0.0.1,localhost",
            "--env=no_proxy=127.0.0.1,localhost",
        ]

    async def call(self, server: str, tool: str, arguments: dict) -> str:
        if server != "blender" or self.client is None:
            raise ValueError("MCP server not admitted")
        result = await self.client.call_tool(tool, arguments)
        return result.model_dump_json()

    async def close(self) -> None:
        # The stdio context owns the client lifetime; outer finally removes this sandbox.
        pass

    async def grade(self, root: Path, task_id: int) -> float:
        path = f"/workspace/mcpuniverse/evaluator/blender/blend_files/tid_{task_id}.blend"
        read = (
            "from pathlib import Path; import base64,json; "
            f"p=Path({path!r}); "
            "assert not p.is_symlink(); "
            "assert not p.exists() or (p.is_file() and p.stat().st_size<64000000); "
            "print(json.dumps({'exists':p.exists(),'artifact':"
            "base64.b64encode(p.read_bytes()).decode() if p.exists() else None}))"
        )
        response = await self.process(["podman", "exec", self.name, "python", "-c", read])
        artifact = json.loads(response)
        if artifact["exists"] is False:
            write_private(self.output.with_suffix(".grader.json"), {"file_exists": False})
            return 0.0
        candidate = base64.b64decode(artifact["artifact"], validate=True)
        with self.output.with_suffix(".blend").open("xb") as file:
            file.write(candidate)
        checker = root / f"mcpuniverse/evaluator/blender/check_functions/tid_{task_id}_check.py"
        script = """import base64,json,subprocess,sys
from pathlib import Path
d=json.load(sys.stdin);r=Path('/tmp/grader');(r/'check_functions').mkdir(parents=True)
p=r/'check_functions'/('tid_'+str(d['task_id'])+'_check.py');p.write_text(d['checker'])
b=r/'candidate.blend';b.write_bytes(base64.b64decode(d['artifact'],validate=True))
with (r/'run.log').open('w') as f:
 subprocess.run(['/opt/blender/blender','--background',str(b),'--python',str(p)],stdout=f,stderr=f,check=True,timeout=120)
print((r/'evaluated_results'/('tid_'+str(d['task_id'])+'_results.json')).read_text())
"""
        payload = json.dumps(
            {
                "artifact": artifact["artifact"],
                "checker": checker.read_text(),
                "task_id": task_id,
            }
        ).encode()
        result = await self.process(
            [
                "podman",
                "run",
                "--rm",
                "-i",
                *self.isolation(),
                "--entrypoint=python",
                self.image,
                "-c",
                script,
            ],
            payload,
        )
        verdict = json.loads(result)
        if type(verdict.get("pass")) is not bool:
            raise ValueError("Official grader returned no boolean verdict")
        write_private(
            self.output.with_suffix(".grader.json"),
            {
                "file_exists": True,
                "official_verdict": verdict,
                "checker_sha256": hashlib.sha256(checker.read_bytes()).hexdigest(),
                "artifact_sha256": hashlib.sha256(candidate).hexdigest(),
            },
        )
        return float(verdict["pass"])


async def execute(args: argparse.Namespace) -> None:
    revision = COMPONENT_BENCHMARKS["mcp-universe"].revision
    checked_checkout(args.upstream_root, revision)
    task = args.upstream_root / (
        f"mcpuniverse/benchmark/configs/mcpuniverse/3d_design/blender_task_{args.task_id:04d}.json"
    )
    spec = json.loads(task.read_text())
    if args.scope == "controlled-batch" and args.plan is None:
        raise ValueError("Controlled batches require the frozen task plan")
    if args.plan:
        plan = json.loads(args.plan.read_text())
        if hashlib.sha256(task.read_bytes()).hexdigest() != plan["task_sha256"].get(task.stem):
            raise ValueError("Task differs from frozen MCP cohort")
        fingerprint = hashlib.sha256(
            json.dumps(plan["task_sha256"], sort_keys=True).encode()
        ).hexdigest()
    else:
        fingerprint = hashlib.sha256(task.read_bytes()).hexdigest()
    transport = BlenderTransport(
        json.loads(args.transport.read_text()) if args.transport else [], args.image, args.output
    )
    command = transport.command(
        [
            "podman",
            "run",
            "--rm",
            "-i",
            "--name",
            transport.name,
            *transport.agent_isolation(args.asset_network, args.asset_proxy),
            args.image,
        ]
    )
    client = RemoteModelClient(
        args.gateway_url, bearer_token=os.environ["AURACLAW_EVAL_GATEWAY_TOKEN"], timeout=180
    )
    config = EvaluationConfig(
        "mcp-universe",
        "mcpuniverse",
        args.model,
        args.revision,
        "sha256:" + fingerprint,
        max_actions=30,
        max_output_tokens=32768,
        timeout_seconds=600,
        seed=20261022,
    )
    try:
        async with stdio_client(
            StdioServerParameters(command=command[0], args=command[1:])
        ) as streams:
            async with ClientSession(*streams) as session:
                transport.client = session
                await session.initialize()
                discovered = await session.list_tools()
                await session.call_tool(
                    "execute_blender_code",
                    {
                        "code": (
                            "import os; os.makedirs('/workspace/mcpuniverse/"
                            "evaluator/blender/blend_files',exist_ok=True)"
                        )
                    },
                )
                tools = [{"server": "blender", **tool.model_dump()} for tool in discovered.tools]

                async def grade(_: str) -> float:
                    return await transport.grade(args.upstream_root, args.task_id)

                env = McpUniverseEnvironment(
                    spec["question"].replace("{{MCPUniverse_DIR}}", "/workspace"),
                    tools=tools,
                    gateway=transport,
                    grade=grade,
                )
                record, trace = await asyncio.wait_for(
                    run_benchmark_episode(
                        env,
                        client,
                        config,
                        task_id=f"blender_task_{args.task_id:04d}",
                        trial=args.trial,
                        profile="auraclaw-mcp-blender-v1",
                    ),
                    timeout=780,
                )
                record.update(
                    validation_scope="official-sdk-adapter",
                    condition="gateway-tools",
                    dataset_revision_verified=True,
                    image_id=args.image,
                    config=asdict(config),
                    run_scope=args.scope,
                    model_snapshot_locked=False,
                    network_scope="restricted-public-assets" if args.asset_network else "offline",
                )
                write_private(args.output.with_suffix(".trace.json"), trace)
                write_private(args.output, record)
                print(
                    json.dumps(
                        {
                            k: record[k]
                            for k in ("status", "success", "official_reward", "tool_calls")
                        }
                    )
                )
    finally:
        await client.aclose()
        await transport.process(["podman", "rm", "--force", "--ignore", transport.name])


def main() -> None:
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("upstream-root", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--plan", type=Path)
    p.add_argument("--transport", type=Path)
    p.add_argument("--asset-network")
    p.add_argument("--asset-proxy")
    for name in ("image", "model", "revision", "gateway-url"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--task-id", type=int, required=True)
    p.add_argument("--trial", type=int, default=0)
    p.add_argument("--scope", choices=("canary", "controlled-batch"), default="canary")
    asyncio.run(execute(p.parse_args()))


if __name__ == "__main__":
    main()
