""" Installer for mqttpublish service.

To uninstall run
wee_extension --uninstall=mqttpublish
"""

import configobj

try:
    # Python 2
    from StringIO import StringIO
except ImportError:
    # Python 3
    from io import StringIO

from weecfg.extension import ExtensionInstaller

VERSION = "0.2.0-rc01"

MQTTPUBLISH_CONFIG = """
[MQTTPublish]
    [[PublishWeeWX]]
        # The MQTT server.
        # Default is localhost.
        host = localhost

        # The port to connect to.
        # Default is 1883.
        port = 1883

        # Maximum period in seconds allowed between communications with the broker.
        # Default is 60.
        keepalive = 60
        
        # username for broker authentication.
        # Default is None.
        username = None

        # password for broker authentication.
        # Default is None.
        password = None
        
        # The binding, loop or archive.
        # Default is: loop
        binding = loop
        
        [[[topics]]]
            [[[[FIRST/REPLACE_ME]]]]

"""

def loader():
    """ Load and return the extension installer. """
    return MQTTPublishInstaller()


class MQTTPublishInstaller(ExtensionInstaller):
    """ The extension installer. """
    def __init__(self):

        install_dict = {
            'version': VERSION,
            'name': 'MQTTPublish',
            'description': 'Publish WeeWX data to a MQTT broker.',
            'author': "Rich Bell",
            'author_email': "bellrichm@gmail.com",
            'files': [('bin/user', ['bin/user/mqttpublish.py'])]
        }


        mqttpublish_dict = configobj.ConfigObj(StringIO(MQTTPUBLISH_CONFIG))
        install_dict['config'] = mqttpublish_dict
        # ToDo: Better service group?
        install_dict['restful_services'] = 'user.mqttpublish.PublishWeeWX'

        super(MQTTPublishInstaller, self).__init__(install_dict)
