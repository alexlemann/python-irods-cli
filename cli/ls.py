from collections import deque
from utils import stringify, get_collection_or_data_object, find_children
from irods_cli import cli
import click
from irods.collection import iRODSCollection


@cli.command()
@click.argument('path')
@click.option('--recursive','-R', is_flag=True,
              help='use a long listing format')
@click.option('--human-readable','-h', is_flag=True,
              help='with -l and/or -s, print human readable sizes' +
              '(e.g., 1K 234M 2G)')
@click.option('--size','-s', is_flag=True,
              help='print the allocated size of each file, in blocks')
@click.option('-l', is_flag=True, help='use a long listing format')
@click.pass_context
def ls(ctx, path, recursive, **print_kwargs):
    obj = get_collection_or_data_object(ctx.obj['session'], path)
    if not obj:
        click.echo('Not found in remote path: {}'.format(path))
        exit(-1)

    children = deque([obj])
    [children.append(c) for c in find_children(obj)]
    while children:
        c = children.pop()
        click.echo(stringify(c, ctx.obj['session'], **print_kwargs))
        if type(c) == iRODSCollection and recursive:
            for child in find_children(c):
                if child:
                    children.append(child)
