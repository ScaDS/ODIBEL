"""The checked-in example config is runnable."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from kg_corrupt.cli import cli

PKG_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG = PKG_ROOT / "configs" / "corruption.example.yaml"
EXAMPLE_ROOT = PKG_ROOT / "fixtures" / "example" / "root_reference.ttl"


def test_example_config_generates_and_validates(tmp_path: Path):
    runner = CliRunner()
    output = tmp_path / "example-corruptions"
    generated = runner.invoke(
        cli,
        ["generate", "--config", str(EXAMPLE_CONFIG), "--output", str(output)],
    )
    assert generated.exit_code == 0, generated.output
    manifest = output / "benchmark_manifest.csv"
    assert manifest.is_file()

    validated = runner.invoke(cli, ["validate", "--manifest", str(manifest)])
    assert validated.exit_code == 0, validated.output

    stats = runner.invoke(cli, ["stats", "--root-reference", str(EXAMPLE_ROOT)])
    assert stats.exit_code == 0, stats.output
    assert "eligible triples: 13" in stats.output
