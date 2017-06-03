import humanize
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist
from irods.data_object import iRODSDataObject
from irods.models import DataObject
from irods.collection import iRODSCollection


def get_collection_or_data_object(session, path):
    obj = None
    try:
        obj = session.data_objects.get(path)
    except DataObjectDoesNotExist:
        pass

    if not obj:
        try:
            obj = session.collections.get(path)
        except CollectionDoesNotExist:
            pass
    return obj


def stringify(obj, session, human_readable, size, l):
    if type(obj) == iRODSCollection:
        return obj.path + '/'
    elif type(obj) == iRODSDataObject:
        if human_readable:
            size = humanize.naturalsize(obj.size, gnu=True)
        else:
            size = obj.size
        if l:
            results = session.query(DataObject.owner_name,
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
        raise ValueError


def find_children(node):
    if type(node) == iRODSDataObject:
        return set()
    else:
        subcollections = set([col for col in node.subcollections])
        return subcollections.union(set([do for do in node.data_objects]))
