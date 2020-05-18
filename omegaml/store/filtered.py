from __future__ import absolute_import

from pymongo.collection import Collection

from omegaml.store import qops
from omegaml.store.query import Filter
from omegaml.util import PickableCollection


class FilteredCollection:
    """
    A permanently filtered collection

    Supports all methods as a Collection does, however any filter or query
    argument is permanently set at instantiation

        fcoll = FilteredCollection(collection, query={ expression })

    Any subsequent operation will automatically apply the query expression.

    Note that v.v. a Collection and all methods that accept a filter as their first
    argument have a changed signature - the filter argument is optional
    with all FilteredCollection methods, as the filter is set at instantiation.

        Example:

            # in pymongo

            filter = {expression}
            coll.find_one_and_replace(filter, replace)

            # FilteredCollection

            coll = FilteredCollection(query={expression})
            coll.find_one_and_replace(replace, filter=None)

    This is so that calls to a FilteredCollection feel more natural, as opposed
    to specifying an empty filter argument on every call. Still, an additional
    filter can be specified on every method that accepts the filter= optional
    argument:

            # temporarily add another filter

            coll.find_one_and_replace(replace, filter={expression})

    Here expression will only apply to this particular method call. The
    global filter set by query= is unchanged.

    If no expression is given, the empty expression {} is assumed. To change
    the expression for the set fcoll.query = { expression }
    """

    def __init__(self, collection, query=None, projection=None, **kwargs):
        is_real_collection = isinstance(collection, Collection)
        while not is_real_collection:
            collection = collection.collection
            is_real_collection = isinstance(collection, Collection)
        collection = PickableCollection(collection)
        query = query or {}
        self._fixed_query = query
        self.projection = projection
        self.collection = collection

    @property
    def _Collection__database(self):
        return self.collection.database

    @property
    def name(self):
        return self.collection.name

    @property
    def database(self):
        return self.collection.database

    @property
    def query(self):
        return Filter(self.collection, **self._fixed_query).query

    def aggregate(self, pipeline, filter=None, **kwargs):
        query = dict(self.query)
        query.update(filter or {})
        pipeline.insert(0, qops.MATCH(query))
        kwargs.update(allowDiskUse=True)
        return self.collection.aggregate(pipeline, **kwargs)

    def find(self, filter=None, **kwargs):
        query = dict(self.query)
        query.update(filter or {})
        return self.collection.find(filter=query, **kwargs)

    def find_one(self, filter=None, *args, **kwargs):
        query = dict(self.query)
        query.update(filter or {})
        return self.collection.find_one(query, *args, **kwargs)

    def count(self, filter=None, **kwargs):
        query = dict(self.query)
        query.update(filter or {})
        return self.collection.count(filter=query, **kwargs)

    def distinct(self, key, filter=None, **kwargs):
        query = dict(self.query)
        query.update(filter or {})
        return self.collection.distinct(key, filter=query, **kwargs)

    def create_index(self, keys, **kwargs):
        return self.collection.create_index(keys, **kwargs)

    def list_indexes(self, **kwargs):
        return self.list_indexes(**kwargs)

    def insert(self, *args, **kwargs):
        raise NotImplementedError(
            "deprecated in Collection and not implemented in FilteredCollection")

    def update(self, *args, **kwargs):
        raise NotImplementedError(
            "deprecated in Collection and not implemented in FilteredCollection")

    def remove(self, *args, **kwargs):
        raise NotImplementedError(
            "deprecated in Collection and not implemented in FilteredCollection")

    def find_and_modify(self, *args, **kwargs):
        raise NotImplementedError(
            "deprecated in Collection and not implemented in FilteredCollection")

    def ensure_index(self, *args, **kwargs):
        raise NotImplementedError(
            "deprecated in Collection and not implemented in FilteredCollection")

    def save(self, *args, **kwargs):
        raise NotImplementedError(
            "deprecated in Collection and not implemented in FilteredCollection")
