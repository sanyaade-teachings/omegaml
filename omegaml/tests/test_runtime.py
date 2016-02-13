import os
from unittest import TestCase

from sklearn.linear_model.base import LinearRegression
from sklearn.utils.validation import NotFittedError

import numpy as np
from omegaml import Omega
from omegaml.util import override_settings, delete_database
import pandas as pd
override_settings(
    OMEGA_MONGO_URL='mongodb://localhost:27017/omegatest',
    OMEGA_MONGO_COLLECTION='store'
)


class RuntimeTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)
        delete_database()

    def tearDown(self):
        TestCase.tearDown(self)

    def test_predict(self):
        # create some data
        x = np.array(range(0, 10))
        y = x * 2
        df = pd.DataFrame({'x': x,
                           'y': y})
        X = df[['x']]
        Y = df[['y']]
        # put into Omega
        os.environ['DJANGO_SETTINGS_MODULE'] = ''
        om = Omega()
        om.runtime.celeryapp.conf.CELERY_ALWAYS_EAGER = True
        om.datasets.put(X, 'datax')
        om.datasets.put(Y, 'datay')
        om.datasets.get('datax')
        om.datasets.get('datay')
        # create a model locally, fit it, store in Omega
        lr = LinearRegression()
        lr.fit(X, Y)
        pred = lr.predict(X)
        om.models.put(lr, 'mymodel')
        self.assertIn('models/mymodel', om.models.list('models/*'))
        # have Omega predict it
        # -- using data already in Omega
        result = om.runtime.model('mymodel').predict('datax')
        pred1 = result.get()
        # -- using data provided locally
        #    note this is the same as
        #        om.datasets.put(X, 'foo')
        #        om.runtime.model('mymodel').predict('foo')
        result = om.runtime.model('mymodel').predict(X)
        pred2 = result.get()
        self.assertTrue(
            (pred == pred1).all(), "runtime prediction is different(1)")
        self.assertTrue(
            (pred == pred2).all(), "runtime prediction is different(2)")

    def test_fit(self):
        # create some data
        x = np.array(range(0, 10))
        y = x * 2
        df = pd.DataFrame({'x': x,
                           'y': y})
        X = df[['x']]
        Y = df[['y']]
        # put into Omega
        os.environ['DJANGO_SETTINGS_MODULE'] = ''
        om = Omega()
        om.runtime.celeryapp.conf.CELERY_ALWAYS_EAGER = True
        om.datasets.put(X, 'datax')
        om.datasets.put(Y, 'datay')
        om.datasets.get('datax')
        om.datasets.get('datay')
        # create a model locally, store (unfitted) in Omega
        lr = LinearRegression()
        om.models.put(lr, 'mymodel2')
        self.assertIn('models/mymodel2', om.models.list('models/*'))
        # predict locally for comparison
        lr.fit(X, Y)
        pred = lr.predict(X)
        # try predicting without fitting
        with self.assertRaises(NotFittedError):
            result = om.runtime.model('mymodel2').predict('datax')
            result.get()
        # have Omega fit the model then predict
        result = om.runtime.model('mymodel2').fit('datax', 'datay')
        result.get()
        # -- using data already in Omega
        result = om.runtime.model('mymodel2').predict('datax')
        pred1 = result.get()
        # -- using data provided locally
        #    note this is the same as
        #        om.datasets.put(X, 'foo')
        #        om.runtime.model('mymodel2').predict('foo')
        result = om.runtime.model('mymodel2').predict(X)
        pred2 = result.get()
        self.assertTrue(
            (pred == pred1).all(), "runtime prediction is different(1)")
        self.assertTrue(
            (pred == pred2).all(), "runtime prediction is different(2)")

    def test_predict_pure_python(self):
        # create some data
        x = np.array(range(0, 10))
        y = x * 2
        df = pd.DataFrame({'x': x,
                           'y': y})
        X = [[x] for x in list(df.x)]
        Y = [[y] for y in list(df.y)]
        # put into Omega -- assume a client with pandas, scikit learn
        os.environ['DJANGO_SETTINGS_MODULE'] = ''
        om = Omega()
        om.runtime.pure_python = True
        om.runtime.celeryapp.conf.CELERY_ALWAYS_EAGER = True
        om.datasets.put(X, 'datax')
        om.datasets.put(Y, 'datay')
        om.datasets.get('datax')
        om.datasets.get('datay')
        # have Omega fit the model then predict
        lr = LinearRegression()
        lr.fit(X, Y)
        pred = lr.predict(X)
        om.models.put(lr, 'mymodel2')
        # -- using data provided locally
        #    note this is the same as
        #        om.datasets.put(X, 'foo')
        #        om.runtime.model('mymodel2').predict('foo')
        result = om.runtime.model('mymodel2').predict(X)
        pred2 = result.get()
        self.assertTrue(
            (pred == pred2).all(), "runtime prediction is different(1)")
        self.assertTrue(
            (pred == pred2).all(), "runtime prediction is different(2)")

    def test_predict_hdf_dataframe(self):
        # create some data
        x = np.array(range(0, 10))
        y = x * 2
        df = pd.DataFrame({'x': x,
                           'y': y})
        X = [[x] for x in list(df.x)]
        Y = [[y] for y in list(df.y)]
        # put into Omega -- assume a client with pandas, scikit learn
        os.environ['DJANGO_SETTINGS_MODULE'] = ''
        om = Omega()
        om.runtime.pure_python = True
        om.runtime.celeryapp.conf.CELERY_ALWAYS_EAGER = True
        om.datasets.put(X, 'datax', as_hdf=True)
        om.datasets.put(Y, 'datay', as_hdf=True)
        # have Omega fit the model then predict
        lr = LinearRegression()
        lr.fit(X, Y)
        pred = lr.predict(X)
        om.models.put(lr, 'mymodel2')
        # -- using data provided locally
        #    note this is the same as
        #        om.datasets.put(X, 'foo')
        #        om.runtime.model('mymodel2').predict('foo')
        result = om.runtime.model('mymodel2').predict(X)
        pred2 = result.get()
        self.assertTrue(
            (pred == pred2).all(), "runtime prediction is different(1)")
        self.assertTrue(
            (pred == pred2).all(), "runtime prediction is different(2)")
