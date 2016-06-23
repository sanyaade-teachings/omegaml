from datetime import datetime
from fnmatch import fnmatch
import glob
import os
import re
from shutil import rmtree
import tempfile
import urlparse
from zipfile import ZipFile, ZIP_DEFLATED

import gridfs
import mongoengine
from mongoengine.errors import DoesNotExist
from mongoengine.fields import GridFSProxy
from ..documents import Metadata

from ..util import (is_estimator, is_dataframe, is_ndarray,
                    settings as omega_settings)


class OmegaStore(object):

    """
    native storage for OmegaML using a mongodb as the storage layer

    An OmegaStore instance is a MongoDB database. It has at least the
    metadata collection which lists all objects stored in it. A metadata
    document refers to the following types of objects (metadata.kind):

    * pandas.dfrows - a Pandas DataFrame stored as a collection of rows
    * sklearn.joblib - a scikit learn estimator/pipline dumped using joblib.dump()
    * python.data - an arbitrary python dict, tuple, list stored as a document

    Note that storing Pandas and scikit learn objects requires the availability
    of the respective packages. If either can not be imported, the OmegaStore
    degrades to a python.data store only. It will still .list() and get() any
    object, however reverts to pure python objects. In this case it is up
    to the client to convert the data into an appropriate format for processing.

    Pandas and scikit-learn objects can only be stored if these packages are
    availables. put() raises a TypeError if you pass such objects and these
    modules cannot be loaded.

    All data are stored within the same mongodb, in per-object collections 
    as follows:

        .metadata     -- all metadata. each object is one document, 
                         see documents.Metadata for details
        .<bucket>.files -- this is the GridFS instance used to store
                         blobs (models, numpy, hdf). The actual file name
                         will be <prefix>/<name>.<ext>, where ext is 
                         optionally generated by put() / get(). 
        .<bucket>.<prefix>.<name>.data
                      -- every other dataset is stored in a separate
                         collection (dataframes, dicts, lists, tuples).
                         Any forward slash in prefix is ignored (e.g. 'data/' 
                         becomes 'data')

        DataFrames by default are stored in their own collection, every
        row becomes a document. To store dataframes as a binary file,
        use put(...., as_hdf=True). .get() will always return a dataframe.

        Python dicts, lists, tuples are stored as a single document with
        a .data attribute holding the JSON-converted representation. .get()
        will always return the corresponding python object of .data. 

        Models are joblib.dump()'ed and ziped prior to transferring into
        GridFs. .get() will always unzip and joblib.load() before returning
        the model. Note this requires that the process using .get() supports
        joblib as well as all python classes referred to. If joblib is not
        supported, .get() returns a file-like object.

        The .metadata entry specifies the format used to store each
        object as well as it's location:

        metadata.kind     -- the type of object
        metadata.name     -- the name of the object, as given on put()
        metadata.gridfile -- the gridfs object (if any, null otherwise)
        metadata.collection -- the name of the collection
        metadata.attributes -- arbitrary custom attributes set in 
                               put(attributes=obj). This is used e.g. by 
                               OmegaRuntime's fit() method to record the
                               data used in the model's training.

        .put() and .get() use helper methods specific to the type in
        object's type and metadata.kind, respectively. In the future 
        a plugin system will enable extension to other types. 
    """

    def __init__(self, mongo_url=None, bucket=None, prefix=None, kind=None):
        """
        :param mongo_url: the mongourl to use for the gridfs
        :param bucket: the mongo collection to use for gridfs
        :param prefix: the path prefix for files. defaults to blank
        :param kind: the kind or list of kinds to limit this store to 
        """
        defaults = omega_settings()
        self.mongo_url = mongo_url or defaults.OMEGA_MONGO_URL
        self.bucket = bucket or defaults.OMEGA_MONGO_COLLECTION
        self._fs = None
        self.tmppath = defaults.OMEGA_TMP
        self.prefix = prefix or ''
        self.force_kind = kind
        # don't initialize db here to avoid using the default settings
        # otherwise Metadata will already have a connection and not use
        # the one provided in override_settings
        self._db = None

    @property
    def mongodb(self):
        if self._db is not None:
            return self._db
        self.parsed_url = urlparse.urlparse(self.mongo_url)
        self.database_name = self.parsed_url.path[1:]
        # connect via mongoengine
        # note this uses a MongoClient in the background, with pooled
        # connections. there are multiprocessing issues with pymongo:
        # http://api.mongodb.org/python/3.2/faq.html#using-pymongo-with-multiprocessing
        # connect=False is due to https://jira.mongodb.org/browse/PYTHON-961
        # this defers connecting until the first access
        # serverSelectionTimeoutMS=2500 is to fail fast, the default is 30000
        self._db = getattr(mongoengine.connect(self.database_name,
                                               host=self.mongo_url,
                                               alias='omega',
                                               connect=False,
                                               serverSelectionTimeoutMS=2500),
                           self.database_name)
        return self._db

    @property
    def fs(self):
        """
        get gridfs instance using url and collection provided
        """
        if self._fs is not None:
            return self._fs
        try:
            self._fs = gridfs.GridFS(self.mongodb, collection=self.bucket)
        except Exception as e:
            raise e
        return self._fs

    def metadata(self, name=None, bucket=None, prefix=None, version=-1):
        """
        return metadata document for the given entry name

        FIXME: version attribute does not do anything
        """
        db = self.mongodb
        fs = self.fs
        prefix = prefix or self.prefix
        bucket = bucket or self.bucket
        # Meta is to silence lint on import error
        Meta = Metadata
        return Meta.objects(name=name, prefix=prefix, bucket=bucket).first()

    def _make_metadata(self, name=None, bucket=None, prefix=None, **kwargs):
        """
        create or update a metadata object

        this retrieves a Metadata object if it exists given the kwargs. Only
        the name, prefix and bucket arguments are considered

        for existing Metadata objects, the attributes kw is treated as follows:

        * attributes=None, the existing attributes are left as is
        * attributes={}, the attributes value on an existing metadata object
        is reset to the empty dict
        * attributes={ some : value }, the existing attributes are updated

        For new metadata objects, attributes defaults to {} if not specified,
        else is set as provided.    

        :param name: the object name
        :param bucket: the bucket, optional, defaults to self.bucket 
        :param prefix: the prefix, optional, defaults to self.prefix
        """
        bucket = bucket or self.bucket
        prefix = prefix or self.prefix
        meta = self.metadata(name=name,
                             prefix=prefix,
                             bucket=bucket)
        if meta:
            for k, v in kwargs.iteritems():
                if k == 'attributes' and v is not None and len(v) > 0:
                    previous = getattr(meta, k, {})
                    previous.update(v)
                    setattr(meta, k, previous)
                elif k == 'attributes' and v is not None and len(v) == 0:
                    setattr(meta, k, {})
                elif k == 'attributes' and v is None:
                    # ignore non specified attributes
                    continue
                else:
                    # by default set whatever attribute is provided
                    setattr(meta, k, v)
        else:
            meta = Metadata(name=name, bucket=bucket, prefix=prefix,
                            **kwargs)
        return meta

    def datastore(self, name=None):
        from warnings import warn
        warn("OmegaStore.datastore() is deprecated, use collection()")
        return self.collection(name=name)

    def collection(self, name=None):
        """
        return a mongo db collection as a datastore

        :param name: the collection to use. if none defaults to the
        collection name given on instantiation. the actual collection name
        used is always prefix + name + '.data'
        """
        collection = self._get_obj_store_key(name, '.datastore')
        collection = collection.replace('..', '.')
        try:
            datastore = getattr(self.mongodb, collection)
        except Exception as e:
            raise e
        return datastore

    def put(self, obj, name, attributes=None, **kwargs):
        """ store an object

        store estimators, pipelines, numpy arrays or pandas dataframes
        """
        if is_estimator(obj):
            return self.put_model(obj, name, attributes)
        elif is_dataframe(obj):
            if kwargs.pop('as_hdf', False):
                return self.put_dataframe_as_hdf(
                    obj, name, attributes, **kwargs)
            elif kwargs.get('groupby'):
                groupby = kwargs.get('groupby')
                return self.put_dataframe_as_dfgroup(
                    obj, name, groupby, attributes)
            append = kwargs.get('append', None)
            timestamp = kwargs.get('timestamp', None)
            index = kwargs.get('index', None)
            return self.put_dataframe_as_documents(
                obj, name, append, attributes, index, timestamp)
        elif is_ndarray(obj):
            return self.put_ndarray_as_hdf(obj, name,
                                           attributes=attributes,
                                           **kwargs)
        elif isinstance(obj, (dict, list, tuple)):
            if kwargs.pop('as_hdf', False):
                self.put_pyobj_as_hdf(obj, name,
                                      attributes=attributes, **kwargs)
            return self.put_pyobj_as_document(obj, name,
                                              attributes=attributes,
                                              **kwargs)
        else:
            raise TypeError('type %s not supported' % type(obj))

    def put_model(self, obj, name, attributes=None):
        """ package model using joblib and store in GridFS """
        zipfname = self._package_model(obj, name)
        with open(zipfname) as fzip:
            fileid = self.fs.put(
                fzip, filename=self._get_obj_store_key(name, 'omm'))
            gridfile = GridFSProxy(grid_id=fileid,
                                   db_alias='omega',
                                   collection_name=self.bucket)
        return self._make_metadata(name=name,
                                   prefix=self.prefix,
                                   bucket=self.bucket,
                                   kind=Metadata.SKLEARN_JOBLIB,
                                   attributes=attributes,
                                   gridfile=gridfile).save()

    def put_dataframe_as_documents(self, obj, name, append=None,
                                   attributes=None, index=None,
                                   timestamp=None):
        """ 
        store a dataframe as a row-wise collection of documents

        :param obj: the dataframe to store
        :param name: the name of the item in the store
        :param append: if False collection will be dropped before inserting,
        if True existing documents will persist. Defaults to True. If not
        specified and rows have been previously inserted, will issue a
        warning.  
        :param index: list of columns, using +, -, @ as a column prefix to
        specify ASCENDING, DESCENDING, GEOSPHERE respectively. For @ the 
        column has to represent a valid GeoJSON object.
        :param timestamp: if True or a field name adds a timestamp. If the
        value is a boolean or datetime, uses _created as the field name. The timestamp
        is always datetime.datetime.utcnow(). May be overriden by specifying
        the tuple (col, datetime). 
        :return: the Metadata object created     
        """
        collection = self.collection(name)
        if append is False:
            collection.drop()
        elif append is None and collection.count(limit=1):
            from warnings import warn
            warn('%s already exists, will append rows' % name)
        if index:
            # get index keys
            if isinstance(index, dict):
                idx_kwargs = index
                index = index.pop('columns')
            else:
                idx_kwargs = {}
            # create index with appropriate options
            from .queryops import MongoQueryOps
            keys, idx_kwargs = MongoQueryOps().make_index(index, **idx_kwargs)
            collection.create_index(keys, **idx_kwargs)
        if timestamp:
            dt = datetime.utcnow()
            if isinstance(timestamp, bool):
                col = '_created'
            elif isinstance(timestamp, basestring):
                col = timestamp
            elif isinstance(timestamp, datetime):
                col, dt = '_created', timestamp
            elif isinstance(timestamp, tuple):
                col, dt = timestamp
            obj[col] = dt
        # bulk insert
        collection.insert_many((row.to_dict() for i, row in obj.iterrows()))
        return self._make_metadata(name=name,
                                   prefix=self.prefix,
                                   bucket=self.bucket,
                                   kind=Metadata.PANDAS_DFROWS,
                                   attributes=attributes,
                                   collection=collection.name).save()

    def get_df_grouped_docs(self, obj, groupby):
        """
        returns a mongo document grouped by the provided columns
        """
        for group_val, gdf in obj.groupby(groupby):
            mongo_doc_dict = {}
            # group_val is a str if only one col is provided
            # is a tuple if more than one cols are provided.
            if isinstance(group_val, tuple):
                pass
            else:
                str_to_tuple = ()
                str_to_tuple += (group_val,)
                group_val = str_to_tuple

            for grouped_cols in groupby:
                mongo_doc_dict[grouped_cols] = group_val[groupby.index(
                    grouped_cols)]

            datacols = list(set(gdf.columns) - set(groupby))
            data_list = []
            for row in gdf[datacols].iterrows():
                row_data_dict = row[1].to_dict()
                for k, v in row_data_dict.iteritems():
                    row_data_dict[k] = v
                data_list.append(row_data_dict)

            mongo_doc_dict['data'] = data_list
            yield mongo_doc_dict

    def put_dataframe_as_dfgroup(self, obj, name, groupby, attributes=None):
        """ store a dataframe grouped by collection in a mongo document """
        datastore = self.datastore(name)
        datastore.drop()

        datastore.insert_many(self.get_df_grouped_docs(obj, groupby))
        return self._make_metadata(name=name,
                                   prefix=self.prefix,
                                   bucket=self.bucket,
                                   kind=Metadata.PANDAS_DFGROUP,
                                   attributes=attributes,
                                   collection=datastore.name).save()

    def put_dataframe_as_hdf(self, obj, name, attributes=None):
        filename = self._get_obj_store_key(name, '.hdf')
        hdffname = self._package_dataframe2hdf(obj, filename)
        with open(hdffname) as fhdf:
            fileid = self.fs.put(fhdf, filename=filename)
        return self._make_metadata(name=name,
                                   prefix=self.prefix,
                                   bucket=self.bucket,
                                   kind=Metadata.PANDAS_HDF,
                                   attributes=attributes,
                                   gridfile=GridFSProxy(grid_id=fileid)).save()

    def put_ndarray_as_hdf(self, obj, name, attributes=None):
        """ store numpy array as hdf

        this is hack, converting the array to a dataframe then storing
        it
        """
        import pandas as pd
        df = pd.DataFrame(obj)
        return self.put_dataframe_as_hdf(df, name, attributes=attributes)

    def put_pyobj_as_hdf(self, obj, name, attributes=None):
        """
        store list, tuple, dict as hdf

        this requires the list, tuple or dict to be convertible into
        a dataframe
        """
        import pandas as pd
        df = pd.DataFrame(obj)
        return self.put_dataframe_as_hdf(df, name, attributes=attributes)

    def put_pyobj_as_document(self, obj, name, attributes=None, append=True):
        """ 
        store a dict as a document

        similar to put_dataframe_as_documents no data will be replaced by
        default. that is, obj is appended as new documents into the objects'
        mongo collection. to replace the data, specify append=False. 
        """
        collection = self.collection(name)
        if append is False:
            collection.drop()
        elif append is None and collection.count(limit=1):
            from warnings import warn
            warn('%s already exists, will append rows' % name)
        objid = collection.insert({'data': obj})
        return self._make_metadata(name=name,
                                   prefix=self.prefix,
                                   bucket=self.bucket,
                                   kind=Metadata.PYTHON_DATA,
                                   collection=collection.name,
                                   attributes=attributes,
                                   objid=objid).save()

    def drop(self, name, force=False, version=-1):
        """
        drop the object

        :param name: the name of the object
        :param force: if True ignores DoesNotExist exception, defaults to False
        meaning this raises a DoesNotExist exception of the name does not 
        exist
        :return: True if object was deleted, False if not. If force is True and
        the object does not exist it will still return True
        """
        meta = self.metadata(name, version=version)
        if meta is None:
            if force:
                # it's gone, so that's what we want
                return True
            else:
                raise DoesNotExist()
        if meta.collection:
            self.mongodb.drop_collection(meta.collection)
            meta.delete()
            return True
        if meta.gridfile is not None:
            meta.gridfile.delete()
            meta.delete()
            return True
        return False

    def get(self, name, version=-1, force_python=False,
            **kwargs):
        """
        retrieve an object

        retrieve estimators, pipelines, data array or pandas dataframe
        previously stored with put()
        """
        meta = self.metadata(name, version=version)
        if meta is None:
            return None
        if not force_python:
            if meta.kind == Metadata.SKLEARN_JOBLIB:
                return self.get_model(name, version=version)
            elif meta.kind == Metadata.PANDAS_DFROWS:
                return self.get_dataframe_documents(name, version=version,
                                                    **kwargs)
            elif meta.kind == Metadata.PANDAS_DFGROUP:
                return self.get_dataframe_dfgroup(
                    name, version=version, **kwargs)
            elif meta.kind == Metadata.PYTHON_DATA:
                return self.get_python_data(name, version=version)
            elif meta.kind == Metadata.PANDAS_HDF:
                return self.get_dataframe_hdf(name, version=version)
        return self.get_object_as_python(meta, version=version)

    def get_model(self, name, version=-1):
        filename = self._get_obj_store_key(name, '.omm')
        packagefname = os.path.join(self.tmppath, name)
        dirname = os.path.dirname(packagefname)
        try:
            os.makedirs(dirname)
        except OSError:
            # OSError is raised if path exists already
            pass
        outf = self.fs.get_version(filename, version=version)
        with open(packagefname, 'w') as zipf:
            zipf.write(outf.read())
        model = self._extract_model(packagefname)
        return model

    def get_dataframe_documents(self, name, columns=None, lazy=False,
                                filter=None, version=-1, **kwargs):
        """
        get dataframe from documents
        """
        collection = self.collection(name)
        if lazy:
            from ..mdataframe import MDataFrame
            filter = filter or kwargs
            df = MDataFrame(collection, columns=columns).query(**filter)
        else:
            import pandas as pd
            filter = filter or kwargs
            if filter:
                from query import Filter
                query = Filter(collection, **filter).query
                cursor = collection.find(filter=query, projection=columns)
            else:
                cursor = collection.find(projection=columns)
            df = pd.DataFrame.from_records(cursor)
            if '_id' in df.columns:
                del df['_id']
        return df

    def rebuild_params(self, kwargs, collection):
        """
        returns a modified set of parameters for querying mongodb
        based on how the mongo document is structured and the
        fields the document is grouped by

        Note: Explicitly to be used only with get_grouped_data only
        """
        modified_params = {}
        db_structure = collection.find_one({}, {'_id': False})
        groupby_columns = list(set(db_structure.keys()) - set(['data']))
        if kwargs is not None:
            for item in kwargs:
                if item not in groupby_columns:
                    modified_query_param = 'data.'+item
                    modified_params[modified_query_param] = kwargs.get(item)
                else:
                    modified_params[item] = kwargs.get(item)
        return modified_params

    def get_grouped_data(self, cursor, kwargs=None):
        """
        yields data from the mongo collection
        """
        kwargs = kwargs if kwargs else {}
        for doc in cursor:
            data = doc.get('data')
            groupby_columns = list(set(doc.keys()) - set(['data']))
            for col in groupby_columns:
                if col in kwargs:
                    kwargs.pop(col)
            col_heads = data[0].keys()
            for col_data in data:
                result_dict = {}
                if kwargs.viewitems() <= col_data.viewitems():
                    for col in col_heads:
                        result_dict[str(col)] = col_data.get(col)
                    for col in groupby_columns:
                        result_dict[str(col)] = doc.get(col)

                    yield result_dict

    def get_dataframe_dfgroup(self, name, version=-1, kwargs=None):
        import pandas as pd
        datastore = self.datastore(name)
        kwargs = kwargs if kwargs else {}
        params = self.rebuild_params(kwargs, datastore)
        cursor = datastore.find(params, {'_id': False})
        df = pd.DataFrame(self.get_grouped_data(cursor, kwargs))
        return df

    def get_dataframe_hdf(self, name, version=-1):
        df = None
        filename = self._get_obj_store_key(name, '.hdf')
        if filename.endswith('.hdf') and self.fs.exists(filename=filename):
            df = self._extract_dataframe_hdf(filename, version=version)
            return df
        else:
            raise gridfs.errors.NoFile(
                "{0} does not exist in mongo collection '{1}'".format(
                    name, self.bucket))

    def get_python_data(self, name, version=-1):
        datastore = self.collection(name)
        cursor = datastore.find()
        data = (d.get('data') for d in cursor)
        return list(data)

    def get_object_as_python(self, meta, version=-1):
        if meta.kind == Metadata.SKLEARN_JOBLIB:
            return meta.gridfile
        if meta.kind == Metadata.PANDAS_HDF:
            return meta.gridfile
        if meta.kind == Metadata.PANDAS_DFROWS:
            return list(getattr(self.mongodb, meta.collection).find())
        if meta.kind == Metadata.PYTHON_DATA:
            col = getattr(self.mongodb, meta.collection)
            return col.find_one(dict(_id=meta.objid)).get('data')
        raise TypeError('cannot return kind %s as a python object' % meta.kind)

    def list(self, pattern=None, regexp=None, kind=None, raw=False):
        """
        list all files in store

        specify pattern as a unix pattern (e.g. 'models/*', 
        or specify regexp)

        :param pattern: the unix file pattern or None for all
        :param regexp: the regexp. takes precedence over pattern
        :param raw: if True return the meta data objects
        """
        db = self.mongodb
        searchkeys = dict(bucket=self.bucket,
                          prefix=self.prefix)
        if kind or self.force_kind:
            kind = kind or self.force_kind
            if isinstance(kind, (tuple, list)):
                searchkeys.update(kind__in=kind)
            else:
                searchkeys.update(kind=kind)
        meta = Metadata.objects(**searchkeys)
        if raw:
            if regexp:
                files = [f for f in meta if re.match(regexp, f.name)]
            elif pattern:
                files = [f for f in meta if fnmatch(f.name, pattern)]
            else:
                files = [f for f in meta]
        else:
            files = [d.name for d in meta]
            if regexp:
                files = [f for f in files if re.match(regexp, f)]
            elif pattern:
                files = [f for f in files if fnmatch(f, pattern)]
            files = [f.replace('.omm', '') for f in files]
        return files

    def _get_obj_store_key(self, name, ext):
        """
        return the store key
        """
        name = '%s.%s' % (name, ext) if not name.endswith(ext) else name
        filename = '{bucket}.{prefix}.{name}'.format(
            bucket=self.bucket,
            prefix=self.prefix,
            name=name,
            ext=ext).replace('/', '_').replace('..', '.')
        return filename

    def _package_model(self, model, filename):
        """
        dump model using joblib and package all joblib files into zip 
        """
        import joblib
        lpath = tempfile.mkdtemp()
        fname = os.path.basename(filename)
        mklfname = os.path.join(lpath, fname)
        zipfname = os.path.join(self.tmppath, fname)
        joblib.dump(model, mklfname)
        with ZipFile(zipfname, 'w', compression=ZIP_DEFLATED) as zipf:
            for part in glob.glob(os.path.join(lpath, '*')):
                zipf.write(part, os.path.basename(part))
        rmtree(lpath)
        return zipfname

    def _extract_model(self, packagefname):
        """
        load model using joblib from a zip file created with _package_model
        """
        import joblib
        lpath = tempfile.mkdtemp()
        fname = os.path.basename(packagefname)
        mklfname = os.path.join(lpath, fname)
        with ZipFile(packagefname) as zipf:
            zipf.extractall(lpath)
        model = joblib.load(mklfname)
        rmtree(lpath)
        return model

    def _package_dataframe2hdf(self, df, filename, key=None):
        """
        package a dataframe as a hdf file
        """
        lpath = tempfile.mkdtemp()
        fname = os.path.basename(filename)
        hdffname = os.path.join(self.tmppath, fname + '.hdf')
        key = key or 'data'
        df.to_hdf(hdffname, key)
        return hdffname

    def _extract_dataframe_hdf(self, filename, version=-1):
        """
        extract a dataframe stored as hdf
        """
        import pandas as pd
        hdffname = os.path.join(self.tmppath, filename)
        dirname = os.path.dirname(hdffname)
        try:
            os.makedirs(dirname)
        except OSError:
            # OSError is raised if path exists already
            pass
        try:
            outf = self.fs.get_version(filename, version=version)
        except gridfs.errors.NoFile, e:
            raise e
        with open(hdffname, 'w') as hdff:
            hdff.write(outf.read())
        hdf = pd.HDFStore(hdffname)
        key = hdf.keys()[0]
        df = hdf[key]
        hdf.close()
        return df
