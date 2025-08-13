import click
from typing import Optional
from .rdf_cmd import rdf_cmd

@click.group()
@click.version_option(version="0.1.0", prog_name="pyodibel")
@click.option(
    "--config", 
    "-c", 
    type=click.Path(exists=True), 
    help="Path to configuration file"
)
@click.option(
    "--verbose", 
    "-v", 
    is_flag=True, 
    help="Enable verbose output"
)
@click.option(
    "--quiet", 
    "-q", 
    is_flag=True, 
    help="Suppress output except errors"
)
@click.pass_context
def cli(ctx: click.Context, config: Optional[str], verbose: bool, quiet: bool):
    """
    KGbench - Knowledge Graph Benchmarking Framework
    
    A comprehensive framework for benchmarking knowledge graph tools
    and creating reproducible pipelines.
    """
    # Ensure context object exists
    ctx.ensure_object(dict)
    
    # Store global options in context
    ctx.obj["config"] = config
    ctx.obj["verbose"] = verbose
    ctx.obj["quiet"] = quiet
    
    # Set up logging level
    if quiet:
        ctx.obj["log_level"] = "ERROR"
    elif verbose:
        ctx.obj["log_level"] = "DEBUG"
    else:
        ctx.obj["log_level"] = "INFO"

cli.add_command(rdf_cmd)

if __name__ == "__main__":
    cli() 