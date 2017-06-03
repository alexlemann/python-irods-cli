import shutil
from irods_cli import cli
import click


@cli.command()
@click.argument('path')
#TODO @click.option('-p', is_flag=True, help='no error if existing, make parent directories as needed')
@click.pass_context
def mkdir(ctx, path):
    ctx.obj['session'].collections.create(path)

@cli.command()
@click.argument('source_path')
@click.argument('destination_collection_path')
#@click.option('--recursive','-R', is_flag=True, help='use a long listing format')
def cp(ctx, source_data_object, destination_path):
    ctx.obj['session'].data_objects.copy(source_data_object, destination_path)

@cli.command()
@click.argument('source_path')
@click.argument('destination_collection_path')
def mv(ctx, source_path, parent_collection_path):
    pass

@cli.command()
@click.argument('path')
#@click.option('--recursive','-R', is_flag=True, help='use a long listing format')
#@click.option('--force','-f', is_flag=True, help='force the operation')
def rm(ctx, path):
    pass

@cli.command()
@click.argument('collection_path')
def rmcol(ctx, collection_path):
    pass

@cli.command()
@click.argument('local_path')
@click.argument('remote_collection_path')
@click.pass_context
def put(ctx, local_path, remote_collection_path):
    filename = os.path.basename(local_path)
    data_object_path = remote_collection_path + '/' + filename
    data_object = ctx.obj['session'].data_objects.create(data_object_path)

    with data_object.open('w') as remote, open(local_path, 'r') as local:
        shutil.copyfileobj(local, remote)
