from stackable import StackableSettings
from stackable.contrib.config.conf_dokku import Config_Dokku

from .env_local import EnvSettings_Local
from stackable.contrib.config.conf_api import Config_ApiKeys
from stackable.contrib.config.conf_whitenoise import Config_DjangoWhitenoise


class EnvSettings_omegamlio(Config_Dokku,
                            Config_ApiKeys,
                            Config_DjangoWhitenoise,
                            EnvSettings_Local):
    ALLOWED_HOSTS = ['omegaml.omegaml.io']

    CONSTANCE_CONFIG = {
        'MONGO_HOST': ('omegaml.omegaml.io:27017', 'mongo db host name'),
        'BROKER_URL': ('amqp://guest@omegaml.omegaml.io:5672//',
                       'rabbitmq broker url'),
        'CELERY_ALWAYS_EAGER': (False, 'if True celery tasks are processed locally'),
    }

    # mail gun email settings
    # see https://app.mailgun.com/app/domains/mg.omegaml.io
    ANYMAIL = {
        # (exact settings here depend on your ESP...)
        "MAILGUN_API_KEY": "key-2u-chky4r83ljbhe522bkmegwixvij46",
        "MAILGUN_SENDER_DOMAIN": 'mg.omegaml.io',  # your Mailgun domain, if needed
    }
    EMAIL_BACKEND = "anymail.backends.mailgun.EmailBackend"  # or sendgrid.EmailBackend, or...
    DEFAULT_FROM_EMAIL = "admin@omegaml.io"  # if you don't already have this in settings

    _addl_apps = ('anymail',)
    StackableSettings.patch_apps(_addl_apps)
