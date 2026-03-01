from click import group, pass_context
import click

from .rdf_cmd import rdf_cmd

@group()
@pass_context
def ops_cmd(ctx: click.Context):
    pass

@ops_cmd.command("list")
@pass_context
def ops_list(ctx: click.Context):
    pass

ops_cmd.add_command(rdf_cmd)