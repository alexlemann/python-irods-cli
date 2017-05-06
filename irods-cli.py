#!/usr/bin/env python
from collections import deque
from getpass import getpass
import logging
import os
import sys

import click
from dotenv import load_dotenv
import humanize

import gevent
import gevent.monkey
from gevent.queue import Queue, PriorityQueue

from irods.exception import CAT_INVALID_AUTHENTICATION, CollectionDoesNotExist, DataObjectDoesNotExist
from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.models import DataObject
from irods.session import iRODSSession

gevent.monkey.patch_all()

# buffer in multiples of 1 MiB
BUFFER_SIZE = int(round((2**20) * .25))
THREADS = 30
WRITE_WORKER_SLEEP = .25
UPDATE_LOOP_SLEEP = 5

logger = logging.getLogger(__name__)


def do_write(args):
    write_queue, total_units, filename = args
    next_work_id = 0
    f = open(filename, 'w')
    logger.debug('Writer: opened file for writing')
    while next_work_id < total_units:
        logger.debug('Writer: {}/{} wq:{}'.format(next_work_id, total_units,
                                                  len(write_queue)))
        if len(write_queue) > 0 and write_queue.peek()[0] == next_work_id:
            work_id, data = write_queue.get()
            f.write(data)
            next_work_id += 1
        else:
            logger.debug("Writer: pause {}/{} wq:{}".format(next_work_id,
                                                            total_units,
                                                            len(write_queue)))
            gevent.sleep(WRITE_WORKER_SLEEP)
    logger.debug('Writer: closing output file')
    f.close()
    logger.debug('Writer: output file closed')


class irodsReader(gevent.Greenlet):
    # Stores a greenlet id (grid), allocated buffer, and connection (data obj)
    def __init__(self, grid, do, buf_size, work, results):
        super(irodsReader, self).__init__()
        self.do = do
        self.grid = grid
        self.work = work
        self.buffer = bytearray(buf_size)
        self.results = results

    def __str__(self):
        return 'Reader: {}'.format(self.grid)

    def _run(self):
        stream = self.do.open('r')
        while not self.work.empty():
            work_id, start_pos = self.work.get()
            logger.debug("Begin read {} work_id:{} pos:{}".format(self,
                                                                  work_id,
                                                                  start_pos))
            stream.seek(start_pos)
            read_s = stream.readinto(self.buffer)
            logger.debug("{} saving results".format(self))
            self.results.put((work_id, self.buffer[:read_s]))
        logger.debug("{} finished work".format(self))

@click.group()
@click.option('--host', default=None, help="irods host")
@click.option('--port', default=None, type=click.INT, help="irods port")
@click.option('--user', default=None, help="irods user")
@click.option('--zone', default=None, help="irods zone")
@click.option('--verbose', is_flag=True, help="verbose output to queue.log")
@click.option('--progress', default=False, is_flag=True, help="show work unit progress")
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
    except CAT_INVALID_AUTHENTICATION:
        click.echo('Authentication failed. Use --verbose to debug.')
        if verbose:
          raise
        exit(-1)
    except:
        click.echo('irods connect failed. Use --verbose to debug.')
        if verbose:
            raise
        exit(-1)


def find_children(node):
    if type(node) == iRODSDataObject:
        return set()
    else:
        subcollections = set([col for col in node.subcollections])
        return subcollections.union(set([do for do in node.data_objects]))

@cli.command()
@click.argument('path')
@click.option('--recursive', is_flag=True, help='use a long listing format')
@click.option('--human-readable','-h', is_flag=True, help='with -l and/or -s, print human readable sizes (e.g., 1K 234M 2G)')
@click.option('--size','-s', is_flag=True, help='print the allocated size of each file, in blocks')
@click.option('-l', is_flag=True, help='use a long listing format')
@click.pass_context
def ls(ctx, path, recursive, **print_kwargs):
    obj = None
    try:
        obj =  ctx.obj['session'].data_objects.get(path)
        print(stringify(obj, ctx.obj['session'], **print_kwargs))
        return
    except DataObjectDoesNotExist:
        pass
    try:
        obj = ctx.obj['session'].collections.get(path)
    except CollectionDoesNotExist:
        if not obj:
            click.echo('Not found in remote path. Use --verbose to debug.')
            click.echo(path)
            if ctx.obj['verbose']:
                raise
            exit(-1)

    children = deque(find_children(obj).union(set([obj])))
    while children:
        c = children.pop()
        print(stringify(c, ctx.obj['session'], **print_kwargs))
        if type(c) == iRODSCollection and recursive:
            for child in find_children(c):
                if child:
                    children.append(child)

@cli.command()
@click.argument('data_object_path')
@click.pass_context
def get(ctx, data_object_path):
    filename = os.path.basename(data_object_path)
    if os.path.isfile(os.path.join(os.path.abspath(os.curdir), filename)):
        click.echo("File already exists locally: {}".format(filename))
        exit(-1)
    try:
        data_object = ctx.obj['session'].data_objects.get(data_object_path)
    except DataObjectDoesNotExist:
        click.echo('No such remote data object. Use --verbose to debug.')
        if ctx.obj['verbose']:
            raise
        exit(-1)

    click.echo('Starting download of {} ({} bytes)'.format(filename,
                                                           data_object.size))

    # Create a full queue of work defined as an work id and the beginning of
    #  file position the read starts at.
    def define_work(full_size, chunk_size):
        work = Queue()
        for work_id, start_pos in enumerate(xrange(0, full_size, chunk_size)):
            work.put((work_id, start_pos))
        return work
    tasks = define_work(data_object.size, BUFFER_SIZE)

    # irodsReaders write results (work id and result byte arrays) to the
    #   results queue the writer greenlet will consume from this queue in
    #   order, pausing if there are any gaps since their last write.
    results = PriorityQueue()
    readers = [irodsReader(i, data_object, BUFFER_SIZE,
                           tasks, results) for i in range(THREADS)]

    t_size = len(tasks)
    writer = gevent.spawn(do_write, (results, t_size, filename))
    [reader.start() for reader in readers]

    # Waiting / progress update loop
    while len(tasks) > 0 or len(results) > 0:
        gevent.sleep(UPDATE_LOOP_SLEEP)
        if progress:
            percent_remaining = round(float(len(tasks))/t_size*100)
            click.echo('Remaining: tq:{} {}% wq:{}'.format(len(tasks),
                                                           percent_remaining,
                                                           len(results)))

    gevent.joinall(readers + [writer])
    click.echo("Wrote {}".format(filename))

def stringify(obj, session, human_readable, size, l):
    if type(obj) == iRODSCollection:
        return obj.path + '/'
    elif type(obj) == iRODSDataObject:
        if human_readable:
            size = humanize.naturalsize(obj.size, gnu=True)
        else:
            size = obj.size
        if l:
            results = session.query(
                                    DataObject.owner_name,
                                    DataObject.modify_time,
                                 ).filter(
                                    DataObject.id == obj.id
                                 ).first()
            return '{} {} {} {}'.format(results[DataObject.owner_name],
                                     size,
                                     results[DataObject.modify_time],
                                     obj.path)
        if size:
            return '{} {}'.format(size, obj.path)
        return obj.path
    elif obj is None:
        return ''
    else:
        logger.debug(type(obj))
        raise ValueError

if __name__ == '__main__':
    dotenv_path = os.path.join(os.path.abspath(os.curdir), '.env')
    load_dotenv(dotenv_path)
    cli(obj={})
