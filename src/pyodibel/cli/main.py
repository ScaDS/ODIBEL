"""
Main CLI entry point for PyODIBEL.

Provides command-line interface for benchmark generation, evaluation,
and data integration tasks.
"""

import click
from pyodibel.cli.rdf_cmd import rdf_group

@click.group()
def cli():
    """PyODIBEL CLI"""
    pass


cli.add_command(rdf_group)

if __name__ == "__main__":
    cli()