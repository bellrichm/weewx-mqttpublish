#    Copyright (c) 2025 Rich Bell <bellrichm@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#

# pylint: disable=wrong-import-order
# pylint: disable=missing-docstring
# pylint: disable=invalid-name

import configobj
import logging

import unittest
import mock

import user.mqttpublish

class TestDeprecatedOptions(unittest.TestCase):
    def test_PublishWeeWX_stanza_is_deprecated(self):
        print("start")

        mock_engine = mock.Mock()
        config_dict = {
            'MQTTPublish': {
                'PublishWeeWX': {
                    'topics': {}
                }
            }
        }
        config = configobj.ConfigObj(config_dict)
        logger = logging.getLogger('user.mqttpublish')
        with mock.patch('user.mqttpublish.mqtt'):
            with mock.patch.object(logger, 'error') as mock_error:
                SUT = user.mqttpublish.MQTTPublish(mock_engine, config)
                SUT.shutDown()
                mock_error.assert_called_once_with("'PublishWeeWX' is deprecated. Move options to top level, '[MQTTPublish]'.")

        print("end")

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(TestDeprecatedOptions('test_PublishWeeWX_stanza_is_deprecated'))
    unittest.TextTestRunner().run(test_suite)

    #unittest.main(exit=False)
