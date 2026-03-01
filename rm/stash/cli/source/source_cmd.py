from click import group, pass_context
import click

from .linkedata_cmd import ld_cmd
from .wikidata_cmd import wikidata_cmd

@group()
@pass_context
def source_cmd(ctx: click.Context):
    pass

@source_cmd.command("list")
@pass_context
def source_list(ctx: click.Context):
    pass


source_cmd.add_command(ld_cmd)
source_cmd.add_command(wikidata_cmd)