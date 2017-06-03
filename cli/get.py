from gevent.queue import Queue, PriorityQueue
import gevent
from irods_cli import cli, logger
import click

from irods.exception import DataObjectDoesNotExist

import os


# buffer in multiples of 1 MiB
BUFFER_SIZE_MB = .25
BUFFER_SIZE = int(round((2**20) * BUFFER_SIZE_MB))
THREADS = 30

WRITE_WORKER_SLEEP = .25
UPDATE_LOOP_SLEEP = 5


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


@cli.command()
@click.argument('data_object_path')
@click.option('--verbose', is_flag=True, help="Show download progress updates.")
@click.pass_context
def get(ctx, data_object_path, progress=True):
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
