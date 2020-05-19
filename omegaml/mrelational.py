import pandas as pd

from omegaml.util import extend_instance


class MRelationalCollection:
    def __init__(self, sql, connection):
        """
        A lazy relational source used with a sqlalchemy connection

        Args:
            sql:
            connection:
        """
        self.sql = sql
        self.connection = connection

    def resolve(self):
        return pd.read_sql(self.sql, self.connection)


class MTable:
    def __init__(self, collection):
        self.collection = collection
        self._applyto = str(self.__class__)
        self._apply_mixins()

    @property
    def value(self):
        return self.collection.resolve()

    def _getcopy_kwargs(self, without=None):
        """ return all parameters required on a copy of this MDataFrame """
        kwargs = dict()
        [kwargs.pop(k) for k in make_tuple(without or [])]
        return kwargs

    def _apply_mixins(self, *args, **kwargs):
        """
        apply mixins in defaults.OMEGA_MREL_MIXINS
        """
        from omegaml import settings
        defaults = settings()
        for mixin, applyto in defaults.OMEGA_MREL_MIXINS:
            if any(v in self._applyto for v in applyto.split(',')):
                extend_instance(self, mixin, *args, **kwargs)
