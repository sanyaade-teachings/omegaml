import pandas as pd

from omegaml.util import extend_instance, PickableAlchemyCollection


class MRelationalCollection:
    def __init__(self, sql, connection, table=None):
        """
        A lazy relational source used with a sqlalchemy connection

        Either
        - give sql, must contain a limit and offset clause
        - give table name, in which case sql alchemy will generate limit and offset clause as required

        Args:
            sql:
            connection:
        """
        self.sql = sql
        self.name = table
        self.connection = PickableAlchemyCollection(connection)

    def resolve(self):
        return pd.read_sql(self.sql, self.connection)

    def execute(self, sql=None):
        stmt = sql or self.sql
        return self.connection.execute(stmt)

    def drop(self):
        return self.connection.execute('delete from {table}'.format(table=self.name))

    def count(self):
        sql = 'select count(*) from ({sql})'.format(sql=self.sql)
        result = self.connection.execute(sql)
        return result.scalar()


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

    def __len__(self):
        return self.collection.count()

    def iterchunks(self, chunksize=1000):
        return self.collection.execute()
