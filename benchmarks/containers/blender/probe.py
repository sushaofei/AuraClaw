"""Protocol and isolation probe; never produces a benchmark score."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def probe(image: str, output: Path) -> None:
    if not image.startswith("sha256:"):
        raise ValueError("An immutable local image ID is required")
    params = StdioServerParameters(
        command="podman",
        args=[
            "run",
            "--rm",
            "-i",
            "--network=none",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--cpus=2",
            "--memory=4g",
            "--pids-limit=256",
            "--tmpfs=/tmp:rw,nosuid,nodev,size=1g,mode=1777",
            "--tmpfs=/workspace:rw,nosuid,nodev,size=512m,mode=1777",
            image,
        ],
    )
    async with stdio_client(params) as streams, ClientSession(*streams) as client:
        await client.initialize()
        tools = await client.list_tools()
        names = {item.name for item in tools.tools}
        assert {"get_scene_info", "execute_blender_code", "get_object_info"} <= names
        code = (
            "import bpy, os\n"
            "assert os.getuid() == 10001\n"
            "assert not os.path.exists('/var/run/docker.sock')\n"
            "assert not os.path.exists('/data4')\n"
            "bpy.ops.mesh.primitive_cube_add()\n"
            "bpy.context.active_object.name = 'AuraClawAdmissionProbe'\n"
            "bpy.ops.wm.save_as_mainfile(filepath='/workspace/admission.blend')\n"
            "assert os.path.getsize('/workspace/admission.blend') > 0\n"
        )
        execute = await client.call_tool("execute_blender_code", {"code": code})
        assert not execute.isError and "Code executed successfully:" in str(execute.content)
        obj = await client.call_tool("get_object_info", {"object_name": "AuraClawAdmissionProbe"})
        assert not obj.isError and "AuraClawAdmissionProbe" in str(obj.content)
        receipt = {
            "scope": "environment-admission-only",
            "image_id": image,
            "mcp_initialize": True,
            "tool_count": len(names),
            "create_inspect_save": True,
            "nonroot": True,
            "network": "none",
            "host_mounts": [],
            "benchmark_score": None,
        }
        with output.open("x") as file:
            json.dump(receipt, file, indent=2)
        print(json.dumps(receipt))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    asyncio.run(asyncio.wait_for(probe(args.image, args.output), timeout=150))


if __name__ == "__main__":
    main()
