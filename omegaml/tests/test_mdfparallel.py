from unittest import TestCase

from omegaml import Omega
from omegaml.mixins.mdf.parallel import ParallelApplyMixin
from omegaml.tests.util import OmegaTestMixin

import pandas as pd

class ParallelMixinTests(OmegaTestMixin, TestCase):
    def setUp(self):
        self.om = Omega()
        self.om.datasets.register_mixin(ParallelApplyMixin)
        self.clean()

    def test_parallel_process(self):
        om = self.om
        large = pd.DataFrame({
            'x': range(1000)
        })
        def myfunc(df):
            df['y'] = df['x'] * 2

        om.datasets.put(large, 'largedf', append=False)
        mdf = om.datasets.getl('largedf')
        mdf.transform(myfunc).persist('largedf_transformed')



