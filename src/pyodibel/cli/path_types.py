"""Click parameter types for local and remote Spark data paths."""

from __future__ import annotations

import click


def is_remote_path(path: str) -> bool:
    return "://" in path and not path.startswith("file://")


def local_path(path: str) -> str:
    return path[7:] if path.startswith("file://") else path


class SparkDataPath(click.ParamType):
    """Accept local paths or remote URIs (hdfs://, s3a://, etc.)."""

    name = "path"

    def __init__(self, *, exists: bool = False, dir_okay: bool = True):
        self._exists = exists
        self._dir_okay = dir_okay

    def convert(self, value, param, ctx):
        path = str(value)
        if is_remote_path(path):
            return path
        return click.Path(exists=self._exists, dir_okay=self._dir_okay).convert(
            local_path(path),
            param,
            ctx,
        )


SPARK_INPUT_PATH = SparkDataPath(exists=True, dir_okay=False)
SPARK_OUTPUT_PATH = SparkDataPath(exists=False, dir_okay=True)
