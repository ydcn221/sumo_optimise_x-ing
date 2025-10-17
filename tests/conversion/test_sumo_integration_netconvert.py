from __future__ import annotations

from pathlib import Path

import pytest

from sumo_optimise.conversion.sumo_integration.netconvert import run_two_step_netconvert


class _StubCompletedProcess:
    def __init__(self) -> None:
        self.returncode = 0
        self.stdout = ""
        self.stderr = ""


@pytest.mark.usefixtures("tmp_path")
def test_run_two_step_netconvert_includes_connections_and_tllogics(monkeypatch, tmp_path: Path) -> None:
    commands: list[list[str]] = []

    def fake_which(_: str) -> str:
        return "netconvert"

    def fake_run(cmd: list[str], **_: object) -> _StubCompletedProcess:
        commands.append(cmd)
        return _StubCompletedProcess()

    monkeypatch.setattr("sumo_optimise.conversion.sumo_integration.netconvert.shutil.which", fake_which)
    monkeypatch.setattr("sumo_optimise.conversion.sumo_integration.netconvert.subprocess.run", fake_run)

    nodes = tmp_path / "net.nod.xml"
    edges = tmp_path / "net.edg.xml"
    connections = tmp_path / "net.con.xml"
    tllogics = tmp_path / "net.tll.xml"

    for path in (nodes, edges, connections, tllogics):
        path.write_text("<xml />", encoding="utf-8")

    run_two_step_netconvert(tmp_path, nodes, edges, connections, tllogics)

    assert len(commands) == 2
    _, step2 = commands
    assert "--connection-files" in step2
    assert connections.name in step2
    assert "--tllogic-files" in step2
    assert tllogics.name in step2
