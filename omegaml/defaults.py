from __future__ import absolute_import
import os
import sys
import six
import yaml

user_homedir = os.path.expanduser('~')

#: configuration file, defaults to $HOME/.omegaml/config.yml
OMEGA_CONFIG_FILE = os.path.join(user_homedir, '.omegaml', 'config.yml')
#: the temp directory used by omegaml processes
OMEGA_TMP = '/tmp'
#: the fully qualified mongodb database URL, including the database name
OMEGA_MONGO_URL = (os.environ.get('OMEGA_MONGO_URL') or
                   os.environ.get('MONGO_URL') or
                   'mongodb://localhost:27017/omega')
#: the collection name in the mongodb used by omegaml storage
OMEGA_MONGO_COLLECTION = 'omegaml'
#: the celery broker name or URL
OMEGA_BROKER = (os.environ.get('OMEGA_BROKER') or
                os.environ.get('RABBITMQ_URL') or
                'amqp://guest@127.0.0.1:5672//')
#: (deprecated) the collection used to store ipython notebooks
OMEGA_NOTEBOOK_COLLECTION = 'ipynb'
#: the celery backend name or URL
OMEGA_RESULT_BACKEND = 'amqp'
#: the celery configurations
OMEGA_CELERY_CONFIG = {
    'CELERY_ACCEPT_CONTENT': ['pickle', 'json', 'msgpack', 'yaml'],
    'BROKER_URL': OMEGA_BROKER,
    'CELERY_RESULT_BACKEND': OMEGA_RESULT_BACKEND,
    'CELERYBEAT_SCHEDULE': {
        'execute_scripts': {
            'task': 'omegajobs.tasks.execute_scripts',
            'schedule': 60,
        },
    },
}
#: storage backends
OMEGA_STORE_BACKENDS = {
    'sklearn.joblib': 'omegaml.backends.ScikitLearnBackend',
    'spark.mllib': 'omegaml.backends.SparkBackend',
    'pandas.csv': 'omegaml.backends.PandasExternalData',
    'python.package': 'omegapkg.PythonPackageData',
}
#: storage mixins
OMEGA_STORE_MIXINS = [
    'omegaml.mixins.store.ProjectedMixin',
    'omegapkg.PythonPackageMixin',
]
#: runtime mixins
OMEGA_RUNTIME_MIXINS = [
    'omegaml.runtime.mixins.ModelMixin',
    'omegaml.runtime.mixins.GridSearchMixin',
]
#: mdataframe mixins
OMEGA_MDF_MIXINS = [
    ('omegaml.mixins.mdf.ApplyMixin', 'MDataFrame,MSeries'),
    ('omegaml.mixins.mdf.FilterOpsMixin', 'MDataFrame,MSeries'),
    ('omegaml.mixins.mdf.apply.ApplyStatistics', 'MDataFrame,MSeries'),
]
#: mdataframe apply context mixins
OMEGA_MDF_APPLY_MIXINS = [
    ('omegaml.mixins.mdf.ApplyArithmetics', 'MDataFrame,MSeries'),
    ('omegaml.mixins.mdf.ApplyDateTime', 'MDataFrame,MSeries'),
    ('omegaml.mixins.mdf.ApplyString', 'MDataFrame,MSeries'),
    ('omegaml.mixins.mdf.ApplyAccumulators', 'MDataFrame,MSeries'),
]

#: the omegaweb url
OMEGA_RESTAPI_URL = (os.environ.get('OMEGA_RESTAPI_URL') or
                     'http://localhost:8000')
#: omega user id
OMEGA_USERID = None
#: omega apikey
OMEGA_APIKEY = None

#: jupyterhub admin user (equals omegajobs.jupyter_config:c.JupyterHub.api_tokens)
OMEGA_JYHUB_USER = os.environ.get('OMEGA_JYHUB_USER', 'jyadmin')
#: jupyterhub admin token (equals omegajobs.jupyter_config:c.JupyterHub.api_tokens)
OMEGA_JYHUB_TOKEN = os.environ.get('OMEGA_JYHUB_TOKEN', '2a67924fa4a9782abe3dd23826a01401833a10f1')
#: jupyterhub url (port equals omegajobs.jupyter_config:c.JupyterHub.hub_port)
OMEGA_JYHUB_URL = os.environ.get('OMEGA_JYHUB_URL', 'http://localhost:8001')
#: omegaweb's API key user by JYHUB user to get another users config. Use omsetupuser to retrieve this key
OMEGA_JYHUB_APIKEY = os.environ.get('OMEGA_JYHUB_APIKEY', '2a67924fa4a9782abe3dd23826a01401833a10f1')

def update_from_config(vars=globals(), config_file=OMEGA_CONFIG_FILE):
    """
    update omegaml.defaults from configuration file

    :param vars: the variables to update
    :param config_file: the path to config.yml or a file object
    :return:
    """
    # override from configuration file
    userconfig = {}
    if isinstance(config_file, six.string_types):
        if os.path.exists(config_file):
            with open(config_file, 'r') as fin:
                userconfig = yaml.load(fin)
    else:
        userconfig = yaml.load(config_file)
    if userconfig:
        for k in [k for k in vars.keys() if k.startswith('OMEGA')]:
            vars[k] = userconfig.get(k, None) or vars[k]
    return vars


def update_from_env(vars=globals()):
    # simple override from env vars
    # -- top-level OMEGA_*
    for k in [k for k in vars.keys() if k.startswith('OMEGA')]:
        vars[k] = os.environ.get(k, None) or vars[k]
    # -- OMEGA_CELERY_CONFIG updates
    for k in [k for k in os.environ.keys() if k.startswith('OMEGA_CELERY')]:
        celery_k = k.replace('OMEGA_', '')
        vars['OMEGA_CELERY_CONFIG'][celery_k] = os.environ[k]
    # -- debug if required
    if '--print-omega-defaults' in sys.argv:
        from pprint import pprint
        vars = {k: v for k, v in six.iteritems(vars) if k.startswith('OMEGA')}
        pprint(vars)
    return vars


def update_from_obj(obj, vars=globals()):
    """
    helper function to update omegaml.defaults from arbitrary module

    :param obj: the source object (must support getattr). Any
       variable starting with OMEGA is set in omegaml.defaults, provided
       it exists there already. 
    """
    for k in [k for k in globals() if k.startswith('OMEGA')]:
        if hasattr(obj, k):
            value = getattr(obj, k)
            vars[k] = value


def update_from_dict(d, vars=globals()):
    """
    helper function to update omegaml.defaults from arbitrary dictionary

    :param d: the source dict (must support [] lookup). Any
       variable starting with OMEGA is set in omegaml.defaults, provided
       it exists there already.
    """
    for k, v in six.iteritems(d):
        if k.startswith('OMEGA'):
            vars[k] = v


# -- test
if any(m in ' '.join(sys.argv) for m in ('unittest', 'test', 'nosetest', 'noserunner')):
    OMEGA_MONGO_URL = OMEGA_MONGO_URL.replace('/omega', '/testdb')
    OMEGA_CELERY_CONFIG['CELERY_ALWAYS_EAGER'] = True
    OMEGA_RESTAPI_URL = ''
else:
    # overrides in actual operations
    # this is to avoid using production settings during test
    update_from_config(globals())
    update_from_env(globals())
