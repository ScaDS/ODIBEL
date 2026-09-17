from __future__ import annotations

from click.testing import CliRunner

from kg_corrupt.cli import cli
from kg_corrupt.config import load_config
from support import people_graph


def test_cli_stats_and_generate_one(tmp_path):
    graph = people_graph()
    root = tmp_path / "root.ttl"
    graph.serialize(root.as_posix(), format="turtle")
    out = tmp_path / "out.nt"
    runner = CliRunner()
    stats = runner.invoke(cli, ["stats", "--root-reference", str(root)])
    assert stats.exit_code == 0, stats.output
    assert "eligible triples:" in stats.output

    one = runner.invoke(
        cli,
        [
            "generate-one",
            "--root-reference",
            str(root),
            "--family",
            "random_fact",
            "--remove-fraction",
            "0.5",
            "--seed",
            "3",
            "--output",
            str(out),
        ],
    )
    assert one.exit_code == 0, one.output
    assert out.is_file()
    assert out.with_suffix(".meta.json").is_file()


def test_cli_generate_and_validate(tmp_path):
    graph = people_graph()
    root = tmp_path / "root.ttl"
    graph.serialize(root.as_posix(), format="turtle")
    cfg_path = tmp_path / "corruption.yaml"
    cfg_path.write_text(
        "\n".join(
            [
                f"input:",
                f"  root_reference: {root}",
                f"output:",
                f"  directory: {tmp_path / 'bench'}",
                "benchmark:",
                "  seeds: [1, 2]",
                "  removal_fractions: [0.5]",
                "families:",
                "  random_fact:",
                "    enabled: true",
                "  entity:",
                "    enabled: false",
                "  predicate:",
                "    enabled: false",
                "  long_tail:",
                "    enabled: false",
                "  source:",
                "    enabled: false",
                "  subgraph:",
                "    enabled: false",
            ]
        ),
        encoding="utf-8",
    )
    runner = CliRunner()
    gen = runner.invoke(cli, ["generate", "--config", str(cfg_path)])
    assert gen.exit_code == 0, gen.output
    manifest = tmp_path / "bench" / "benchmark_manifest.csv"
    val = runner.invoke(cli, ["validate", "--manifest", str(manifest)])
    assert val.exit_code == 0, val.output
    summary = runner.invoke(cli, ["summarize", "--manifest", str(manifest)])
    assert summary.exit_code == 0, summary.output


def test_load_config_resolves_relative_paths(tmp_path):
    root = tmp_path / "root.ttl"
    people_graph().serialize(root.as_posix(), format="turtle")
    cfg = tmp_path / "sub" / "cfg.yaml"
    cfg.parent.mkdir()
    cfg.write_text("input:\n  root_reference: ../root.ttl\n", encoding="utf-8")
    loaded = load_config(cfg)
    assert loaded.input.root_reference == root.resolve()
