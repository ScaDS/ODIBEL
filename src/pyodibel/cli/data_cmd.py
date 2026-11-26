"""
Dataset command group for PyODIBEL CLI.

Thin wrappers around `pyodibel.datasets.manifest` for ``.odibel`` metadata.
"""

import click

from pyodibel.datasets.manifest import CHECKSUM_FILE, META_DIR, verify, write_checksums
from pyodibel.datasets.versions import VersionHistory, commit, status


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


_CHECKSUM_OPTION = click.option(
    "--checksum", is_flag=True, help="Re-hash every file instead of trusting the stat cache."
)


@data_group.command("status")
@click.argument("dataset_dirs", nargs=-1, required=True, type=click.Path(exists=True, file_okay=False))
@_CHECKSUM_OPTION
def status_cmd(dataset_dirs: tuple[str, ...], checksum: bool):
    """Show payload changes since the latest version (or files.sha256)."""
    for dataset_dir in dataset_dirs:
        report = status(dataset_dir, checksum=checksum)
        base = f"version {report.version}" if report.version else CHECKSUM_FILE
        summary = f"{sum(report.changes.counts().values())} changes" if report.changes else "clean"
        click.echo(f"{dataset_dir}: {summary} against {base} (hashed {report.hashed} of {report.files} files)")
        for kind, names in (
            ("added", report.changes.added),
            ("removed", report.changes.removed),
            ("modified", report.changes.modified),
        ):
            for name in names:
                click.echo(f"  {kind:<9}{name}")


@data_group.command("commit")
@click.argument("dataset_dir", type=click.Path(exists=True, file_okay=False))
@click.option("-m", "--message", required=True, help="What changed in this version.")
@click.option("--label", "labels", multiple=True, help="Point a label (e.g. stable) at the new version.")
@click.option("--allow-empty", is_flag=True, help="Record a version even without payload changes.")
@_CHECKSUM_OPTION
def commit_cmd(dataset_dir: str, message: str, labels: tuple[str, ...], allow_empty: bool, checksum: bool):
    """Record the current payload of DATASET_DIR as a new version."""
    try:
        entry = commit(dataset_dir, message, labels=labels, checksum=checksum, allow_empty=allow_empty)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    changes = ", ".join(f"{n} {kind}" for kind, n in entry["changes"].items() if n) or "no changes"
    click.echo(f"version {entry['version']}: {entry['files']} files, {changes}")


@data_group.command("log")
@click.argument("dataset_dir", type=click.Path(exists=True, file_okay=False))
def log_cmd(dataset_dir: str):
    """List the recorded versions of DATASET_DIR."""
    history = VersionHistory.load(dataset_dir)
    if not history.versions:
        click.echo("no versions")
        return
    labels_by_version: dict[int, list[str]] = {}
    for label, version in history.labels.items():
        labels_by_version.setdefault(int(version), []).append(label)
    for entry in reversed(history.versions):
        number = int(entry["version"])
        tags = ", ".join(["latest"] * (entry is history.latest) + sorted(labels_by_version.get(number, [])))
        c = entry["changes"]
        changes = f"+{c['added']} -{c['removed']} ~{c['modified']}"
        click.echo(f"v{number}  {entry['created']}  {entry['files']} files  {changes}" + (f"  [{tags}]" if tags else ""))
        click.echo(f"     {entry['message']}")
