from stackable.contrib.config.conf_dokku import Config_Dokku

from .env_local import EnvSettings_Local
from stackable.contrib.config.conf_api import Config_ApiKeys
from stackable.contrib.config.conf_whitenoise import Config_DjangoWhitenoise


class EnvSettings_dokku(Config_Dokku,
                        Config_ApiKeys,
                        Config_DjangoWhitenoise,
                        EnvSettings_Local):
    ALLOWED_HOSTS = ['omegaml.dokku.me']
