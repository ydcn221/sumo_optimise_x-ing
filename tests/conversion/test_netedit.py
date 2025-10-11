from __future__ import annotations

from pathlib import Path

from sumo_optimise.conversion.sumo_integration.netedit import launch_netedit


def test_launch_netedit_skips_when_binary_missing(monkeypatch, caplog, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "sumo_optimise.conversion.sumo_integration.netedit.shutil.which",
        lambda _: None,
    )

    network_file = tmp_path / "network.net.xml"
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
    network_file = outdir / "network.net.xml"
    network_file.touch()

    launch_netedit(network_file)

    assert calls == [([exe_path, str(network_file)], str(outdir))]
