import os
from unittest import TestCase

from sklearn.linear_model.base import LinearRegression

import numpy as np
from omega import Omega
import pandas as pd
from sklearn.utils.validation import NotFittedError


class RuntimeTests(TestCase):

    def setUp(self):
        TestCase.setUp(self)

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
        self.assertIn('models/mymodel', om.models.list('models/*'))
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
