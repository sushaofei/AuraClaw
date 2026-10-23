import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def worker(monkeypatch):
    root = Path(__file__).parents[2] / "benchmarks"
    monkeypatch.syspath_prepend(str(root))
    spec = importlib.util.spec_from_file_location("skills_worker", root / "run_skillsbench.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_skill_metadata_does_not_disclose_body_and_paths_stay_in_package(worker, tmp_path):
    folder = tmp_path / "skills" / "routing"
    folder.mkdir(parents=True)
    (folder / "SKILL.md").write_text("---\ndescription: Route planning\n---\nPRIVATE SKILL BODY")
    (tmp_path / "private.txt").write_text("outside")
    (folder / "escape.txt").symlink_to(tmp_path / "private.txt")
    skills = worker.SkillFiles(tmp_path / "skills")
    assert "PRIVATE SKILL BODY" not in str(await skills.list_skills())
    assert "PRIVATE SKILL BODY" in await skills.read("routing", "SKILL.md")
    with pytest.raises(ValueError):
        await skills.read("routing", "escape.txt")
    with pytest.raises(ValueError):
        await skills.read("routing", "../../private.txt")


def test_skill_metadata_symlink_rejected(worker, tmp_path):
    folder = tmp_path / "skills" / "bad"
    folder.mkdir(parents=True)
    (tmp_path / "private.txt").write_text("outside")
    (folder / "SKILL.md").symlink_to(tmp_path / "private.txt")
    with pytest.raises(ValueError):
        worker.SkillFiles(tmp_path / "skills")
