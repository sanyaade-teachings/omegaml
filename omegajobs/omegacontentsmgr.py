from IPython.utils import tz
import nbformat
from notebook.services.contents.manager import ContentsManager
from tornado import web

import omegaml as om
from omegajobs.omegacheckpoints import OmegaStoreContentsCheckpoints


class OmegaStoreContentsManager(ContentsManager):

    def __init__(self, **kwargs):
        super(OmegaStoreContentsManager, self).__init__(**kwargs)

    def _checkpoints_class_default(self):
        return OmegaStoreContentsCheckpoints

    @property
    def omega(self):
        return om

    @property
    def store(self):
        return self.omega.jobs.store

    def get(self, path, content=True, type=None, format=None):
        path = path.strip('/')
        if not self.exists(path):
            raise web.HTTPError(404, u'No such file or directorys: %s' % path)

        if path == '':
            if type not in (None, 'directory'):
                raise web.HTTPError(400, u'%s is a directory, not a %s' % (
                    path, type), reason='bad type')
            model = self._dir_model(path, content=content)
        elif type == 'notebook' or (type is None and path.endswith('.ipynb')):
            model = self._notebook_model(path, content=content)
        else:
            raise web.HTTPError(400, u'%s is not a directory' % path,
                                reason='bad type')
        return model

    def save(self, model, path):
        path = path.strip('/')
        if 'type' not in model:
            raise web.HTTPError(400, u'No file type provided')
        if 'content' not in model and model['type'] != 'directory':
            raise web.HTTPError(400, u'No file content provided')

        self.run_pre_save_hook(model=model, path=path)
        om = self.omega

        try:
            if model['type'] == 'notebook':
                nb = nbformat.from_dict(model['content'])
                self.check_and_sign(nb, path)
                om.jobs.put(nb, path)
            else:
                raise web.HTTPError(
                    400, "Unhandled contents type: %s" % model['type'])
        except web.HTTPError:
            raise
        except Exception as e:
            self.log.error(
                u'Error while saving file: %s %s', path, e, exc_info=True)
            raise web.HTTPError(
                500, u'Unexpected error while saving file: %s %s' % (path, e))

        if model['type'] == 'notebook':
            self.validate_notebook_model(model)
            validation_message = model.get('message', None)

        model = self.get(path, content=False)
        if validation_message:
            model['message'] = validation_message

        return model

    def delete_file(self, path):
        path = path.strip('/')
        om = self.omega
        om.jobs.drop(path)

    def rename_file(self, old_path, new_path):
        old_path = old_path.strip('/')
        new_path = new_path.strip('/')
        if self.file_exists(new_path):
            raise web.HTTPError(409, u'Notebook already exists: %s' % new_path)
        # rename on metadata. Note the gridfile instance stays the same
        meta = om.jobs.metadata(old_path)
        meta.name = new_path
        meta.save()

    def exists(self, path):
        """
        Does a file or dir exist at the given collection in gridFS?
        We do not have dir so dir_exists returns true.
        Parameters
        ----------
        path : string
            The relative path to the file's directory (with '/' as separator)
        Returns
        -------
        exists : bool
            Whether the target exists.
        """
        path = path.strip('/')
        return self.file_exists(path) or self.dir_exists(path)

    def dir_exists(self, path=''):
        path = path.strip('/')
        om = self.omega
        if path == '':
            return True
        return len(om.jobs.list('{path}.*'.format(path=path))) > 0

    def file_exists(self, path):
        path = path.strip('/')
        om = self.omega
        return path in om.jobs.list(path)

    def is_hidden(self, path):
        return False

    def _read_notebook(self, path, as_version=None):
        path = path.strip('/')
        om = self.omega
        return om.jobs.get(path)

    def _notebook_model(self, path, content=True):
        """
        Build a notebook model
        if content is requested, the notebook content will be populated
        as a JSON structure (not double-serialized)
        """
        path = path.strip('/')
        model = self._base_model(path)
        model['type'] = 'notebook'
        if content:
            nb = self._read_notebook(path, as_version=4)
            self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            self.validate_notebook_model(model)
        # if exists already fake last modified and created timestamps
        # otherwise jupyter notebook will claim a newer version "on disk"
        if self.exists(path):
            model['last_modified'] = tz.datetime(1970, 1, 1)
            model['created'] = tz.datetime(1970, 1, 1)
        return model

    def _base_model(self, path):
        """Build the common base of a contents model"""
        # http://jupyter-notebook.readthedocs.io/en/stable/extending/contents.html
        path = path.strip('/')
        last_modified = tz.utcnow()
        created = tz.utcnow()
        # Create the base model.
        model = {}
        model['name'] = path.rsplit('/', 1)[-1]
        model['path'] = path
        model['last_modified'] = last_modified
        model['created'] = created
        model['content'] = None
        model['format'] = None
        model['mimetype'] = None
        model['writable'] = True
        return model

    def _dir_model(self, path, content=True):
        """
        Build a model to return all of the files in gridfs
        if content is requested, will include a listing of the directory
        """
        path = path.strip('/')
        model = self._base_model(path)
        model['type'] = 'directory'
        model['content'] = contents = []
        om = self.omega
        entries = om.jobs.list('{path}.*'.format(path=path), raw=True)
        for meta in entries:
            try:
                entry = self.get(meta.name, content=content)
            except:
                msg = ('_dir_model error, cannot get {}, '
                       'removing from list'.format(meta.name))
                self.log.warning(msg)
            else:
                contents.append(entry)
        model['format'] = 'json'
        return model
