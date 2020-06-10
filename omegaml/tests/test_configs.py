from unittest import TestCase

import six
from mock import patch

from omegaml.defaults import update_from_config, update_from_obj, update_from_dict


class BareObj(object):
    pass

class ConfigurationTests(TestCase):
    def setUp(self):
        self.defaults = {
            'OMEGA_MONGO_URL': 'foo'
        }

    def test_update_from_configfile(self):
        """
        Test updates from config file works
        """
        data = """
        OMEGA_MONGO_URL: updated-foo
        NOTOMEGA_VALUE: some other value
        """
        cfgfile = six.StringIO(data)
        cfgfile.seek(0)
        update_from_config(self.defaults, cfgfile)
        self.assertIn('OMEGA_MONGO_URL', self.defaults)
        self.assertEqual(self.defaults['OMEGA_MONGO_URL'], 'updated-foo')
        self.assertNotIn('NOTOMEGA_VALUE', self.defaults)

    def test_update_from_obj(self):
        data = BareObj()
        data.OMEGA_MONGO_URL = 'updated-foo'
        update_from_obj(data, self.defaults)
        self.assertIn('OMEGA_MONGO_URL', self.defaults)
        self.assertEqual(self.defaults['OMEGA_MONGO_URL'], 'updated-foo')
        self.assertNotIn('NOTOMEGA_VALUE', self.defaults)

    def test_update_from_dict(self):
        data = {
            'OMEGA_MONGO_URL': 'updated-foo'
        }
        update_from_dict(data, self.defaults)
        self.assertIn('OMEGA_MONGO_URL', self.defaults)
        self.assertEqual(self.defaults['OMEGA_MONGO_URL'], 'updated-foo')
        self.assertNotIn('NOTOMEGA_VALUE', self.defaults)

    def test_config_from_apikey(self):
        """
        Test an Omega instance can be created from user specific configs
        """
        import omegaml as om
        from omegaml.util import settings
        # check we get default without patching
        defaults = settings(reload=True)
        setup = om.setup
        with patch('omegaml._base_config') as defaults:
            defaults.MY_OWN_SETTING = 'foo'
            om = om.setup()
            self.assertEqual(om.defaults.MY_OWN_SETTING, 'foo')
        # reset om.datasets to restored defaults
        om = setup()
        self.assertNotEqual(om.datasets.mongo_url, 'foo')
        # now test we can change the default through config
        # we patch the actual api call to avoid having to set up the user db
        # the objective here is to test get_omega_from_apikey
        with patch('omegaml.client.userconf.get_user_config_from_api') as mock:
            mock.return_value = {
                'objects': [
                   {
                        'data': {
                            'OMEGA_MONGO_URL': 'updated-foo',
                            'OMEGA_MY_OWN_SETTING': 'updated-foo',
                        }
                    }
                ]
            }
            from omegaml import _base_config as _real_base_config
            with patch('omegaml._base_config') as defaults:
                from omegaml.client.userconf import get_omega_from_apikey
                # link callbacks used by get_omega_from_api_key
                defaults.update_from_dict = _real_base_config.update_from_dict
                defaults.load_user_extensions = _real_base_config.load_user_extensions
                defaults.load_framework_support = _real_base_config.load_framework_support
                defaults.OMEGA_MY_OWN_SETTING = 'foo'
                om = get_omega_from_apikey('foo', 'bar')
                self.assertEqual(om.defaults.OMEGA_MY_OWN_SETTING, 'updated-foo')
                self.assertEqual(om.datasets.mongo_url, 'updated-foo')
        # restore defaults
        defaults = settings(reload=True)
        om = setup()
        self.assertNotEqual(om.datasets.mongo_url, 'foo')

