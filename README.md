# A Python and Gevent based iRODS client

The beginnings of a gevent based iRODS client using Python. Currently it only
supports downloading.

To get started:
1. ``pip install -r requirements.txt``
2. ./irods-cli.py --help

You can configure the irods connection on the command line via (eg ``--zone``)
in environment variables (eg ``IRODS_HOST``) or via a ``.env`` file in your
current working directory.

Sample ``.env`` file. **Be sure to set permissions carefully on the file.**
```shell
IRODS_HOST=irods.example.org
IRODS_PORT=1247
IRODS_USER=my_irods_user
IRODS_ZONE=testZone
IRODS_PASSWORD=secretPassword
```

A password cannot be specified via a command line option and must be passed via
the environment or entered when prompted.
