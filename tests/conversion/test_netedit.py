from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sumo_optimise.conversion.sumo_integration import netedit
from sumo_optimise.conversion.sumo_integration.netedit import launch_netedit
from sumo_optimise.conversion.utils import constants


def test_launch_netedit_skips_when_binary_missing(monkeypatch, caplog, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "sumo_optimise.conversion.sumo_integration.netedit.shutil.which",
        lambda _: None,
    )

    network_file = tmp_path / constants.NETWORK_FILE_NAME
    network_file.touch()

    with caplog.at_level("INFO"):
        launch_netedit(network_file)

    assert "Skip launching netedit" in caplog.text


def test_launch_netedit_invokes_subprocess(monkeypatch, tmp_path: Path) -> None:
    exe_path = "/usr/bin/netedit"

    monkeypatch.setattr(
        "sumo_optimise.conversion.sumo_integration.netedit.shutil.which",
        lambda _: exe_path,
    )

    calls: list[tuple[list[str], str]] = []

    def fake_popen(cmd: list[str], cwd: str) -> object:  # pragma: no cover - trivial
        calls.append((cmd, cwd))
        return object()

    monkeypatch.setattr(
        "sumo_optimise.conversion.sumo_integration.netedit.subprocess.Popen",
        fake_popen,
    )

    outdir = tmp_path / "out"
    outdir.mkdir()
    network_file = outdir / constants.NETWORK_FILE_NAME
    network_file.touch()

    launch_netedit(network_file)

    assert calls == [([exe_path, str(network_file.resolve())], str(outdir.resolve()))]


class DummyProcess:
    def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - construction only
        self.args = args
        self.kwargs = kwargs


def test_launch_netedit_uses_resolved_network_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    network_file = tmp_path / "nested" / constants.NETWORK_FILE_NAME
    network_file.parent.mkdir(parents=True)
    network_file.touch()

    popen_args: dict[str, Any] = {}

    monkeypatch.setattr(netedit.shutil, "which", lambda _: "/usr/bin/netedit")
    monkeypatch.setattr(
        netedit.subprocess,
        "Popen",
        lambda cmd, cwd: popen_args.update({"cmd": cmd, "cwd": cwd}) or DummyProcess(),
    )

    netedit.launch_netedit(network_file)

    assert popen_args["cmd"][1] == str(network_file.resolve())
    assert popen_args["cwd"] == str(network_file.resolve().parent)
