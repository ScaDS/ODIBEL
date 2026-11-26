"""
Dataset command group for PyODIBEL CLI.

Thin wrappers around `pyodibel.datasets.manifest` for ``.odibel`` metadata.
"""

import click

from pyodibel.datasets.manifest import CHECKSUM_FILE, META_DIR, verify, write_checksums


@click.group("data")
def data_group():
    """Dataset metadata (.odibel) commands."""


@data_group.command("hash")
@click.argument("dataset_dir", type=click.Path(exists=True, file_okay=False))
def hash_cmd(dataset_dir: str):
    """Write .odibel/files.sha256 for all payload files in DATASET_DIR."""
    checksums = write_checksums(dataset_dir)
    click.echo(f"{len(checksums)} files -> {dataset_dir}/{META_DIR}/{CHECKSUM_FILE}")


@data_group.command("verify")
@click.argument("dataset_dirs", nargs=-1, required=True, type=click.Path(exists=True, file_okay=False))
@click.option(
    "--metadata-only",
    is_flag=True,
    help="Validate dataset.yaml and files.sha256 without reading the payload.",
)
def verify_cmd(dataset_dirs: tuple[str, ...], metadata_only: bool):
    """Check manifest fields and payload checksums of one or more datasets."""
    failed = False
    for dataset_dir in dataset_dirs:
        report = verify(dataset_dir, metadata_only=metadata_only)
        label = report.dataset_id or dataset_dir
        if report.ok:
            click.echo(f"ok      {label}")
            continue
        failed = True
        click.echo(f"FAILED  {label}")
        for problem in report.problems:
            click.echo(f"  {problem}")
        for kind, names in (
            ("missing", report.missing),
            ("checksum mismatch", report.mismatched),
            ("not in files.sha256", report.unlisted),
        ):
            for name in names:
                click.echo(f"  {kind}: {name}")
    if failed:
        raise SystemExit(1)
