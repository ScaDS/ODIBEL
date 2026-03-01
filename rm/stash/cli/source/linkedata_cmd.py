from click import group, option, pass_context
import click

from pyodibel.source.linked_data import LinkedDataSource
from pyodibel.api.data_source import DataSourceConfig

@group()
@pass_context
def ld_cmd(ctx: click.Context):
    pass

@ld_cmd.command("crawl")
@option("--seed-uri", type=str, required=True)
@option("--max-depth", type=int, default=0)
@pass_context
def ld_crawl(ctx: click.Context, seed_uri: str, max_depth: int):
    source = LinkedDataSource(DataSourceConfig(name="linked_data"))
    for graph in source.crawl([seed_uri], max_depth=max_depth):
        print(graph)

@ld_cmd.command("fetch")
@option("--uri", type=str, required=True)
@pass_context
def ld_fetch(ctx: click.Context, uri: str):
    source = LinkedDataSource(DataSourceConfig(name="linked_data"), max_depth=0, follow_links=False)
    for graph in source.fetch(uri):
        print(graph.serialize(format="turtle"))
