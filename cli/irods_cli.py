#!/usr/bin/env python
from getpass import getpass
import logging
import os
import sys

import click
from dotenv import load_dotenv

import gevent.monkey

from irods.exception import CAT_INVALID_AUTHENTICATION
from irods.session import iRODSSession


gevent.monkey.patch_all()

logger = logging.getLogger(__name__)


@click.group()
@click.option('--host', default=None, help="irods host")
@click.option('--port', default=None, type=click.INT, help="irods port")
@click.option('--user', default=None, help="irods user")
@click.option('--zone', default=None, help="irods zone")
@click.option('--verbose', is_flag=True, help="verbose output to queue.log")
@click.option('--progress', default=False, is_flag=True,
              help="show work unit progress")
@click.pass_context
def cli(ctx, host, port, user, zone, verbose, progress):
    ctx.obj['progress'] = progress
    ctx.obj['verbose'] = verbose
    if verbose:
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    if host is None:
        host = os.getenv('IRODS_HOST', None)
    if port is None:
        port = int(os.getenv('IRODS_PORT', None))
    if user is None:
        user = os.getenv('IRODS_USER', None)
    if zone is None:
        zone = os.getenv('IRODS_ZONE', None)
    password = os.getenv('IRODS_PASSWORD', None)
    if password is None:
        password = getpass()
    try:
        ctx.obj['session'] = iRODSSession(host=host, port=port, user=user,
                                          zone=zone, password=password)
    except CAT_INVALID_AUTHENTICATION as e:
        click.echo('Authentication failed. Use --verbose to debug.')
        if verbose:
            raise e
        exit(-1)
    except Exception as e:
        click.echo('irods connect failed. Use --verbose to debug.')
        if verbose:
            raise e
        exit(-1)


def main():
    dotenv_path = os.path.join(os.path.abspath(os.curdir), '.env')
    load_dotenv(dotenv_path)
    cli(obj={})

if __name__ == '__main__':
    main()
