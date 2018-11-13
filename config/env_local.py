
import os
from stackable.contrib.config.conf_allauth import Config_DjangoAllAuth
from stackable.contrib.config.conf_bootstrap import Config_Bootstrap3
from stackable.contrib.config.conf_cities_light import Config_Cities_Light
from stackable.contrib.config.conf_constance import Config_DjangoConstance
from stackable.contrib.config.conf_debugtoolbar import Config_DebugToolbar
from stackable.contrib.config.conf_djangoadmin import Config_DjangoAdmin
from stackable.contrib.config.conf_djangonose import Config_DjangoNoseTests
from stackable.contrib.config.conf_payment import Config_DjangoPayments
from stackable.contrib.config.conf_postoffice import Config_DjangoPostOffice
from stackable.contrib.config.conf_sekizai import Config_DjangoSekizai
from stackable.contrib.config.conf_whitenoise import Config_DjangoWhitenoise
from stackable.contrib.config.email.filebased import Config_FileEmail
from stackable.stackable import StackableSettings

from config.env_global import EnvSettingsGlobal


class EnvSettings_Local(Config_DjangoWhitenoise,
                        Config_DjangoNoseTests,
                        Config_DjangoSekizai,
                        Config_Bootstrap3,
                        Config_DjangoPayments,
                        Config_DjangoConstance,
                        Config_FileEmail,
                        # Config_DebugToolbar,
                        Config_Cities_Light,
                        Config_DjangoAllAuth,
                        Config_DjangoAdmin,
                        Config_DjangoPostOffice,
                        EnvSettingsGlobal):
    _prefix_apps = ('omegaweb', 'landingpage', 'paasdeploy', 'orders')
    _addl_apps = ('tastypie',
                  'tastypie_swagger',
                  'tastypiex',
                  'organizations',
                  'django_extensions',
                  )
    StackableSettings.patch_apps(_prefix_apps, at='django.contrib.staticfiles')
    StackableSettings.patch_apps(_addl_apps)

    API_CONFIG = {
        'apis': (
            ('omegaweb', 'omegaweb.api.v1_api'),
        ),
    }

    BASE_MONGO_URL = 'mongodb://{user}:{password}@{mongohost}/{dbname}'
    MONGO_ADMIN_URL = BASE_MONGO_URL.format(user='admin',
                                            mongohost='localhost:27019',
                                            password='foobar',
                                            dbname='admin')

    OMEGA_MONGO_URL = (os.environ.get('MONGO_URL') or
                       BASE_MONGO_URL.format(user='admin',
                                             mongohost='localhost:27019',
                                             password='foobar',
                                             dbname='userdb'))

    SITE_ID = 1

    CONSTANCE_CONFIG = {
        'MONGO_HOST': ('localhost:27019', 'mongo db host name'),
        'BROKER_URL': ('amqp://guest@127.0.0.1:5672//', 'rabbitmq broker url'),
        'CELERY_ALWAYS_EAGER': (True, 'if True celery tasks are processed locally'),
    }

    DEBUG = True

    ALLOWED_HOSTS = ['localhost', 'testserver']

    STATICFILES_STORAGE = 'omegaweb.util.FailsafeCompressedManifestStaticFilesStorage'

    OMEGA_JYHUB_URL = 'http://localhost:5000'
    OMEGA_JYHUB_USER = os.environ.get('OMEGA_JYHUB_USER', 'jyadmin')
    OMEGA_JYHUB_TOKEN = os.environ.get('OMEGA_JYHUB_TOKEN', 'PQZ4Sw2YNvNpdnwbLetbDDDF6NcRbazv2dCL')
    OMEGA_RESTAPI_URL = 'http://localhost:8000'

    OMEGA_CELERY_IMPORTS = ['omegaml.tasks', 'omegaee.tasks', 'omegajobs.tasks', 'omegapkg.tasks']

    #: storage backends
    OMEGA_STORE_BACKENDS = {
        'sklearn.joblib': 'omegaml.backends.ScikitLearnBackend',
        'spark.mllib': 'omegaee.backends.SparkBackend',
        'pandas.csv': 'omegaee.backends.PandasExternalData',
        'python.package': 'omegapkg.PythonPackageData',
    }
    #: storage mixins
    OMEGA_STORE_MIXINS = [
        'omegaml.mixins.store.ProjectedMixin',
        'omegapkg.PythonPackageMixin',
    ]

