"""
Publish to MQTT.
Supports publishing "immediately" on loop or archive creation.
And/Or publishing from an externa/persistent queue.

Configuration:
[MQTTPublish]
    [[PublishWeeWX]]
        # Whether the service is enabled or not.
        # Valid values: True or False
        # Default is True.
        enable = True

        # The binding, loop or archive.
        # Default is loop.
        # Only used by the service.
        binding = loop

        # Controls the MQTT logging.
        # Default is false.
        log = false

        # The clientid to connect with.
        # Service default is MQTTSubscribeService-xxxx.
        # Driver default is MQTTSubscribeDriver-xxxx.
        #    Where xxxx is a random number between 1000 and 9999.
        clientid =

        # The MQTT server.
        # Default is localhost.
        host = localhost

        # The port to connect to.
        # Default is 1883.
        port = 1883

        # The protocol to use
        # Valid values: MQTTv31, MQTTv311
        # Default is MQTTv311,
        protocol = MQTTv311

        # Maximum period in seconds allowed between communications with the broker.
        # Default is 60.
        keepalive = 60

        # username for broker authentication.
        # Default is None.
        username = None

        # password for broker authentication.
        # Default is None.
        password = None

        [[[lwt]]]
            # The topic that the will message should be published on.
            # Default is 'status'.
            topic = 'status'

            # Default is 'online'.
            online_payload ='online'

            # The message to send as a will.
            # Default is 'offline'.
            offline_payload = offline

            # he quality of service level to use for the will.
            # Default is 0
            qos = 0

            # If set to true, the will message will be set as the "last known good"/retained message for the topic.
            # The default is True.
            retain = True

        [[[Topics]]]
            [[[[first/topic]]]]
            # Controls if the topic is published.
            # Default is True.
            publish = True

            # The QOS level to subscribe to.
            # Default is 0
            qos = 0

            # The MQTT retain flag.
            # The default is False.
            retain = False

            # Controls if the unit label is appended to the field name.
            # Default is True.
            append_unit_label = True

            # The unit system for data published to this topic.
            # The default is US.
            unit_system = US

            # The aggregations to perform
            [[[[[aggregates]]]]]
                # The name of the observation in the MQTT payload.
                # This can be any name. For example: rainSumDay, outTempMinHour, etc
                [[[[[[aggregateObservationName]]]]]]
                    # The WeeWX observation to aggregate, rain, outTemp, etc,
                    observation =

                    # The type of aggregation to perform.
                    # See, https://www.weewx.com/docs/customizing.htm#aggregation_types
                    aggregation = max

                    # The time period over which the aggregation shoulf occurr.
                    # Valid values: hour, day, week, month, year, yesterday, last24hours, last7days, last31days, last366days
                    period =
"""

import queue as Queue

import abc
import datetime
import json
import logging
import random
import ssl
import threading
import time
import traceback

import configobj
import paho.mqtt.client as mqtt

import weeutil
from weeutil.weeutil import to_bool, to_float, to_int, TimeSpan

import weewx
from weewx.engine import StdService

VERSION = "1.0.0-rc01a"

log = logging.getLogger(__name__)
def setup_logging(logging_level, config_dict):
    """ Setup logging for running in standalone mode."""
    if logging_level:
        weewx.debug = logging_level

    weeutil.logger.setup('wee_MQTTSS', config_dict)

def logdbg(msg):
    """ Log debug level. """
    log.debug(msg)

def loginf(msg):
    """ Log informational level. """
    log.info(msg)

def logerr(msg):
    """ Log error level. """
    log.error(msg)

# need to rethink
# pylint: disable=unnecessary-lambda
period_timespan = {
    'hour': lambda time_stamp: weeutil.weeutil.archiveHoursAgoSpan(time_stamp),
    'day': lambda time_stamp: weeutil.weeutil.archiveDaySpan(time_stamp),
    'yesterday': lambda time_stamp: weeutil.weeutil.archiveDaySpan(time_stamp, 1),
    'week': lambda time_stamp: weeutil.weeutil.archiveWeekSpan(time_stamp),
    'month': lambda time_stamp: weeutil.weeutil.archiveMonthSpan(time_stamp),
    'year': lambda time_stamp: weeutil.weeutil.archiveYearSpan(time_stamp),
    'last24hours': lambda time_stamp: TimeSpan(time_stamp, time_stamp - 86400),
    'last7days': lambda time_stamp: TimeSpan(time_stamp,
                                                time.mktime((datetime.date.fromtimestamp(time_stamp) - \
                                                            datetime.timedelta(days=7)).timetuple())),
    'last31days': lambda time_stamp: TimeSpan(time_stamp,
                                                time.mktime((datetime.date.fromtimestamp(time_stamp) - \
                                                            datetime.timedelta(days=31)).timetuple())),
    'last366days': lambda time_stamp: TimeSpan(time_stamp,
                                                time.mktime((datetime.date.fromtimestamp(time_stamp) - \
                                                            datetime.timedelta(days=366)).timetuple()))
}
# pylint: enable=unnecessary-lambda

def gettid():
    """Get TID as displayed by htop.
       This is architecture dependent."""
    import ctypes # want to keep this 'local' pylint: disable=import-outside-toplevel
    from ctypes.util import find_library # want to keep this 'local' pylint: disable=import-outside-toplevel
    libc = ctypes.CDLL(find_library('c'))

    for cmd in (186, 224, 178):
        tid = ctypes.CDLL(libc).syscall(cmd)
        if tid != -1:
            return tid

    return 0

class AbstractPublisher(abc.ABC):
    """ Managing publishing to MQTT. """
    def __init__(self, publisher, service_dict):
        self.connected = False
        self.mqtt_logger = {
            mqtt.MQTT_LOG_INFO: loginf,
            mqtt.MQTT_LOG_NOTICE: loginf,
            mqtt.MQTT_LOG_WARNING: loginf,
            mqtt.MQTT_LOG_ERR: logerr,
            mqtt.MQTT_LOG_DEBUG: logdbg
            }

        self.publisher = publisher

        self.max_retries = to_int(service_dict.get('max_retries', 5))
        self.retry_wait = to_int(service_dict.get('retry_wait', 5))
        log_mqtt = to_bool(service_dict.get('log', False))
        self.host = service_dict.get('host', 'localhost')
        self.keepalive = to_int(service_dict.get('keepalive', 60))
        self.port = to_int(service_dict.get('port', 1883))
        username = service_dict.get('username', None)
        password = service_dict.get('password', None)
        clientid = service_dict.get('clientid', 'MQTTPublish-' + str(random.randint(1000, 9999)))

        protocol_string = service_dict.get('protocol', 'MQTTv311')
        self.protocol = getattr(mqtt, protocol_string, 0)

        loginf(f"host is {self.host}")
        loginf(f"port is {self.port}")
        loginf(f"keepalive is {self.keepalive}")
        loginf(f"protocol is {self.protocol}")
        loginf(f"username is {username}")
        if password is not None:
            loginf("password is set")
        else:
            loginf("password is not set")
            loginf(f"clientid is {clientid}")

        self.client = self.get_client(clientid, self.protocol)
        self.set_callbacks(log_mqtt)

        if username is not None and password is not None:
            self.client.username_pw_set(username, password)

        tls_dict = service_dict.get('tls')
        if tls_dict:
            self.config_tls(tls_dict)

        self.lwt_dict = service_dict.get('lwt')
        if self.lwt_dict:
            self.client.will_set(topic=self.lwt_dict.get('topic', 'status'),
                                 payload=self.lwt_dict.get('offline_payload', 'offline'),
                                 qos=to_int(self.lwt_dict.get('qos', 0)),
                                 retain=to_bool(self.lwt_dict.get('retain', True)))

        self._connect()

    @classmethod
    def get_publisher(cls, publisher, service_dict):
        ''' Factory method to get appropriate MQTTPublish for paho mqtt version. '''
        if hasattr(mqtt, 'CallbackAPIVersion'):
            protocol_string = service_dict.get('protocol', 'MQTTv311')
            protocol = getattr(mqtt, protocol_string, 0)
            if protocol in [mqtt.MQTTv31, mqtt.MQTTv311]:
                return PublisherV2MQTT3(publisher, service_dict)

            return PublisherV2(publisher, service_dict)

        return PublisherV1(publisher, service_dict)

    def _connect(self):
        try:
            self.connect(self.host, self.port, self.keepalive)
        except Exception as exception: # (want to catch all) pylint: disable=broad-except
            logerr(f"MQTT connect failed with {type(exception)} and reason {exception}.")
            logerr(f"{traceback.format_exc()}")
        retries = 0
        # loop seems to break before connect, perhaps due to logging
        self.client.loop(timeout=0.1)
        time.sleep(1)
        while not self.connected:
            logdbg( "waiting")
            # loop seems to break before connect, perhaps due to logging
            self.client.loop(timeout=0.1)
            time.sleep(5)

            retries += 1
            if retries > self.max_retries:
                # Shut thread down, a bit of a hack
                self.publisher.running = False
                return

            try:
                self.connect(self.host, self.port, self.keepalive)
            except Exception as exception: # (want to catch all) pylint: disable=broad-except
                logerr(f"MQTT connect failed with {type(exception)} and reason {exception}.")
                logerr(f"{traceback.format_exc()}")

    def _reconnect(self):
        logdbg("*** Before reconnect ***")
        self.client.reconnect()
        logdbg("*** After reconnect ***")
        retries = 0
        logdbg("*** Before loop ***")
        self.client.loop(timeout=1.0)
        logdbg("*** After loop ***")
        while not self.connected:
            logdbg("waiting")
            self.client.loop(timeout=5.0)

            retries += 1
            if retries > self.max_retries:
                # Shut thread down, a bit of a hack
                self.publisher.running = False
                return

            self.client.reconnect()

        loginf("reconnected")

    def config_tls(self, tls_dict):
        """ Configure TLS."""
        valid_cert_reqs = {
            'none': ssl.CERT_NONE,
            'optional': ssl.CERT_OPTIONAL,
            'required': ssl.CERT_REQUIRED
        }

        # Some versions are dependent on the OpenSSL install
        valid_tls_versions = {}
        try:
            valid_tls_versions['tls'] = ssl.PROTOCOL_TLS
        except AttributeError:
            pass
        try:
            valid_tls_versions['tlsv1'] = ssl.PROTOCOL_TLSv1
        except AttributeError:
            pass
        try:
            valid_tls_versions['tlsv11'] = ssl.PROTOCOL_TLSv1_1
        except AttributeError:
            pass
        try:
            valid_tls_versions['tlsv12'] = ssl.PROTOCOL_TLSv1_2
        except AttributeError:
            pass
        try:
            valid_tls_versions['sslv2'] = ssl.PROTOCOL_SSLv2
        except AttributeError:
            pass
        try:
            valid_tls_versions['sslv23'] = ssl.PROTOCOL_SSLv23
        except AttributeError:
            pass
        try:
            valid_tls_versions['sslv3'] = ssl.PROTOCOL_SSLv3
        except AttributeError:
            pass

        ca_certs = tls_dict.get('ca_certs')

        valid_cert_reqs = valid_cert_reqs.get(tls_dict.get('certs_required', 'required'))
        if valid_cert_reqs is None:
            raise ValueError(f"Invalid 'certs_required'., {tls_dict['certs_required']}")

        tls_version = valid_tls_versions.get(tls_dict.get('tls_version', 'tlsv12'))
        if tls_version is None:
            raise ValueError(f"Invalid 'tls_version'., {tls_dict['tls_version']}")

        self.client.tls_set(ca_certs=ca_certs,
                            certfile=tls_dict.get('certfile'),
                            keyfile=tls_dict.get('keyfile'),
                            cert_reqs=valid_cert_reqs,
                            tls_version=tls_version,
                            ciphers=tls_dict.get('ciphers'))

    def publish_message(self, time_stamp, qos, retain, topic, data):
        """ Publish the message. """
        # pylint: disable=too-many-arguments
        if not self.connected:
            self._reconnect()
        mqtt_message_info = self.client.publish(topic, data, qos=qos, retain=retain)
        logdbg(f"Publishing ({int(time.time())}): {int(time_stamp)} {mqtt_message_info.mid} {qos} {topic}")

        self.client.loop(timeout=0.1)

    def get_client(self, client_id, protocol):
        ''' Get the MQTT client. '''
        raise NotImplementedError("Method 'get_client' is not implemented")

    def set_callbacks(self, log_mqtt):
        ''' Setup the MQTT callbacks. '''
        raise NotImplementedError("Method 'set_callbacks' is not implemented")

    def connect(self, host, port, keepalive):
        ''' Connect to the MQTT server. '''
        raise NotImplementedError("Method 'connect' is not implemented")

class PublisherV1(AbstractPublisher):
    ''' MQTTPublish that communicates with paho mqtt v1.'''
    def __init__(self, publisher, service_dict):
        protocol_string = service_dict.get('protocol', 'MQTTv311')
        protocol = getattr(mqtt, protocol_string, 0)
        if protocol not in [mqtt.MQTTv31, mqtt.MQTTv311]:
            raise ValueError(f"Invalid protocol, {protocol_string}.")

        super().__init__(publisher, service_dict)

    def get_client(self, client_id, protocol):
        ''' Get the MQTT client. '''
        return mqtt.Client(client_id=client_id, protocol=protocol) # (v1 signature) pylint: disable=no-value-for-parameter

    def set_callbacks(self, log_mqtt):
        ''' Setup the MQTT callbacks. '''
        if log_mqtt:
            self.client.on_log = self.on_log

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

    def connect(self, host, port, keepalive):
        ''' Connect to the MQTT server. '''
        self.client.connect(host, port, keepalive)

    def on_log(self, _client, _userdata, level, msg):
        """ The on_log callback. """
        self.mqtt_logger[level](f"MQTT log: {msg}")

    def on_connect(self, _client, _userdata, flags, rc):
        """ The on_connect callback. """
        # https://pypi.org/project/paho-mqtt/#on-connect
        # rc:
        # 0: Connection successful
        # 1: Connection refused - incorrect protocol version
        # 2: Connection refused - invalid client identifier
        # 3: Connection refused - server unavailable
        # 4: Connection refused - bad username or password
        # 5: Connection refused - not authorised
        # 6-255: Currently unused.
        loginf(f"Connected with result code {int(rc)}, {mqtt.error_string(rc)}")
        loginf(f"Connected flags {str(flags)}")
        if self.lwt_dict:
            self.client.publish(topic=self.lwt_dict.get('topic', 'status'),
                                 payload=self.lwt_dict.get('online_payload', 'online'),
                                 qos=to_int(self.lwt_dict.get('qos', 0)),
                                 retain=to_bool(self.lwt_dict.get('retain', True)))
        self.connected = True

    def on_disconnect(self, _client, _userdata, rc):
        """ The on_connect callback. """
        # https://pypi.org/project/paho-mqtt/#on-discconnect
        # The rc parameter indicates the disconnection state.
        # If MQTT_ERR_SUCCESS (0), the callback was called in response to a disconnect() call.
        # If any other value the disconnection was unexpected,
        # such as might be caused by a network error.
        if rc == 0:
            loginf(f"Disconnected with result code {int(rc)}, {mqtt.error_string(rc)}")
        else:
            logerr(f"Disconnected with result code {int(rc)}, {mqtt.error_string(rc)}")

        # As of 1.6.1, Paho MQTT cannot have a callback invoke a second callback. So we won't attempt to reconnect here.
        # Because that would cause the on_connect callback to be called. Instead we will just mark as not connected.
        # And check the flag before attempting to publish.
        self.connected = False

    def on_publish(self, _client, _userdata, mid):
        """ The on_publish callback. """
        time_stamp = "          "
        qos = ""
        logdbg(f"Published  ({int(time.time())}): {time_stamp} {mid} {qos}")

class PublisherV2MQTT3(AbstractPublisher):
    ''' MQTTPublish that communicates with paho mqtt v2. '''
    def get_client(self, client_id, protocol):
        ''' Get the MQTT client. '''
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, # (only available in v2) pylint: disable=unexpected-keyword-arg
                            protocol=protocol,
                            client_id=client_id,
                            clean_session=True)

    def set_callbacks(self, log_mqtt):
        ''' Setup the MQTT callbacks. '''
        if log_mqtt:
            self.client.on_log = self.on_log

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

    def connect(self, host, port, keepalive):
        ''' Connect to the MQTT server. '''
        self.client.connect(host=host, port=port, keepalive=keepalive)

    def on_log(self, _client, _userdata, level, msg):
        """ The on_log callback. """
        self.mqtt_logger[level](f"MQTT log: {msg}")

    def on_connect(self, _client, _userdata, flags, reason_code, _properties):
        """ The on_connect callback. """
        loginf(f"Connected with result code {int(int(reason_code.value))}")
        loginf(f"Connected flags {str(flags)}")
        if self.lwt_dict:
            self.client.publish(topic=self.lwt_dict.get('topic', 'status'),
                                 payload=self.lwt_dict.get('online_payload', 'online'),
                                 qos=to_int(self.lwt_dict.get('qos', 0)),
                                 retain=to_bool(self.lwt_dict.get('retain', True)))
        self.connected = True

    def on_disconnect(self, _client, _userdata, _flags, reason_code, _properties):
        """ The on_disconnect callback. """
        # https://pypi.org/project/paho-mqtt/#on-discconnect
        # The rc parameter indicates the disconnection state.
        # If MQTT_ERR_SUCCESS (0), the callback was called in response to a disconnect() call.
        # If any other value the disconnection was unexpected,
        # such as might be caused by a network error.
        if int(reason_code.value) == 0:
            loginf(f"Disconnected with result code {int(int(reason_code.value))}")
        else:
            logerr(f"Disconnected with result code {int(int(reason_code.value))}")

        # ToDo: research how it works with v2
        # As of 1.6.1, Paho MQTT cannot have a callback invoke a second callback. So we won't attempt to reconnect here.
        # Because that would cause the on_connect callback to be called. Instead we will just mark as not connected.
        # And check the flag before attempting to publish.
        self.connected = False

    def on_publish(self, _client, _userdata, mid, _reason_codes, _properties):
        """ The on_publish callback. """
        time_stamp = "          "
        qos = ""
        logdbg(f"Published  ({int(time.time())}): {time_stamp} {mid} {qos}")

class PublisherV2(AbstractPublisher):
    ''' MQTTPublish that communicates with paho mqtt v2. '''
    def get_client(self, client_id, protocol):
        ''' Get the MQTT client. '''
        return mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, # (only available in v2) pylint: disable=unexpected-keyword-arg
                            protocol=protocol,
                            client_id=client_id)

    def set_callbacks(self, log_mqtt):
        ''' Setup the MQTT callbacks. '''
        if log_mqtt:
            self.client.on_log = self.on_log

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

    def connect(self, host, port, keepalive):
        ''' Connect to the MQTT server. '''
        self.client.connect(host=host, port=port, keepalive=keepalive, clean_start=True)

    def on_log(self, _client, _userdata, level, msg):
        """ The on_log callback. """
        self.mqtt_logger[level](f"MQTT log: {msg}")

    def on_connect(self, _client, _userdata, flags, reason_code, _properties):
        """ The on_connect callback. """
        loginf(f"Connected with result code {int(int(reason_code.value))}")
        loginf(f"Connected flags {str(flags)}")
        if self.lwt_dict:
            self.client.publish(topic=self.lwt_dict.get('topic', 'status'),
                                 payload=self.lwt_dict.get('online_payload', 'online'),
                                 qos=to_int(self.lwt_dict.get('qos', 0)),
                                 retain=to_bool(self.lwt_dict.get('retain', True)))
        self.connected = True

    def on_disconnect(self, _client, _userdata, _flags, reason_code, _properties):
        """ The on_disconnect callback. """
        # https://pypi.org/project/paho-mqtt/#on-discconnect
        # The rc parameter indicates the disconnection state.
        # If MQTT_ERR_SUCCESS (0), the callback was called in response to a disconnect() call.
        # If any other value the disconnection was unexpected,
        # such as might be caused by a network error.
        if int(reason_code.value) == 0:
            loginf(f"Disconnected with result code {int(int(reason_code.value))}")
        else:
            logerr(f"Disconnected with result code {int(int(reason_code.value))}")

        # ToDo: research how it works with v2
        # As of 1.6.1, Paho MQTT cannot have a callback invoke a second callback. So we won't attempt to reconnect here.
        # Because that would cause the on_connect callback to be called. Instead we will just mark as not connected.
        # And check the flag before attempting to publish.
        self.connected = False

    def on_publish(self, _client, _userdata, mid, _reason_codes, _properties):
        """ The on_publish callback. """
        time_stamp = "          "
        qos = ""
        logdbg(f"Published  ({int(time.time())}): {time_stamp} {mid} {qos}")


class PublishWeeWX():
    """ Backwards compatibility class."""
    def __init__(self, engine, config_dict):
        self.mqtt_publish = MQTTPublish(engine, config_dict)

    def shutDown(self): # need to override parent - pylint: disable=invalid-name
        """Run when an engine shutdown is requested."""
        self.mqtt_publish.shutDown()

class MQTTPublish(StdService):
    """ A service to publish WeeWX loop and/or archive data to MQTT. """
    def __init__(self, engine, config_dict):
        super(MQTTPublish, self).__init__(engine, config_dict)

        self.service_dict = config_dict.get('MQTTPublish', {})
        #  backwards compatability
        if 'PublishWeeWX' in self.service_dict.sections:
            logerr("'PublishWeeWX' is deprecated. Move options to top level, '[MQTTPublish]'.")
            self.service_dict = config_dict.get('MQTTPublish', {}).get('PublishWeeWX', {})

        self.enable = to_bool(self.service_dict.get('enable', True))
        if not self.enable:
            loginf("Not enabled, exiting.")
            return

        # todo - make configurable
        self.kill_weewx = []
        self.max_thread_restarts = 2
        self.thread_restarts = 0

        # todo, tie this into the topic bindings somehow...
        binding = weeutil.weeutil.option_as_list(self.service_dict.get('binding', ['archive', 'loop']))

        self.data_queue = Queue.Queue()

        if 'loop' in binding:
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        if 'archive' in binding:
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        self._thread = PublishWeeWXThread(self.service_dict, self.data_queue)
        self.thread_start()

        #logdbg("Threadid of PublishWeeWX is: %s" % gettid())

    def thread_start(self):
        """Start the publishing thread."""
        loginf("starting thread")
        self._thread.start()
        # ToDo - configure how long to wait for thread to start
        self.thread_start_wait = 5.0
        loginf("joining thread")
        #self._thread.join(self.thread_start_wait)
        loginf("joined thread")

        if not self._thread.is_alive():
            loginf("oh no")
            raise weewx.WakeupError("Unable to start MQTT publishing thread.")

        loginf("started thread")

    def new_loop_packet(self, event):
        """ Handle loop packets. """
        self._handle_record('loop', event.packet)

    def new_archive_record(self, event):
        """ Handle archive records. """
        self._handle_record('archive', event.record)

    def _handle_record(self, data_type, data):
        if not self._thread.running:
            if self.thread_restarts < self.max_thread_restarts:
                self.thread_restarts += 1
                self._thread = PublishWeeWXThread(self.service_dict, self.data_queue)
                self.thread_start()

                self.data_queue.put({'time_stamp': data['dateTime'], 'type': data_type, 'data': data})
                self._thread.threading_event.set()
            elif 'threadEnded' in self.kill_weewx:
                raise weewx.StopNow("MQTT publishing thread has stopped.")
        else:
            self.data_queue.put({'time_stamp': data['dateTime'], 'type': data_type, 'data': data})
            self._thread.threading_event.set()

    def shutDown(self): # need to override parent - pylint: disable=invalid-name
        """Run when an engine shutdown is requested."""
        loginf("SHUTDOWN - initiated")
        if self._thread:
            loginf("SHUTDOWN - thread initiated")
            self._thread.running = False
            self._thread.threading_event.set()
            self._thread.join(20.0)
            if self._thread.is_alive():
                logerr(f"Unable to shut down {self._thread.name} thread")

            self._thread = None

class PublishWeeWXThread(threading.Thread):
    """Publish WeeWX data to MQTT. """
    # pylint: disable=too-many-instance-attributes
    UNIT_REDUCTIONS = {
        'degree_F': 'F',
        'degree_C': 'C',
        'inch': 'in',
        'mile_per_hour': 'mph',
        'mile_per_hour2': 'mph',
        'km_per_hour': 'kph',
        'km_per_hour2': 'kph',
        'knot': 'knot',
        'knot2': 'knot2',
        'meter_per_second': 'mps',
        'meter_per_second2': 'mps',
        'degree_compass': None,
        'watt_per_meter_squared': 'Wpm2',
        'uv_index': None,
        'percent': None,
        'unix_epoch': None,
        }
    def __init__(self, service_dict, data_queue):
        threading.Thread.__init__(self)

        self.publisher = None
        self.running = False

        self.db_manager = None

        self.service_dict = service_dict

        exclude_keys = ['password']
        sanitized_service_dict = {k: self.service_dict[k] for k in set(list(self.service_dict.keys())) - set(exclude_keys)}
        logdbg(f"sanitized configuration removed {exclude_keys}")
        logdbg(f"sanitized_service_dict is {sanitized_service_dict}")

        self.topics_loop, self.topics_archive = self.configure_topics(self.service_dict)
        self.wait_before_retry = float(self.service_dict.get('wait_before_retry', 2))
        self.keepalive = to_int(self.service_dict.get('keepalive', 60))


        loginf(f"Wait before retry is {int(self.wait_before_retry)}")

        self.data_queue = data_queue
        self.threading_event = threading.Event()

    def configure_fields(self, fields_dict, ignore, publish_none_value, append_unit_label, conversion_type, format_string):
        """ Configure the fields. """
        # pylint: disable=too-many-arguments
        fields = {}
        for field in fields_dict.sections:
            fields[field] = {}
            field_dict = fields_dict.get(field, {})
            fields[field]['name'] = field_dict.get('name', field)
            fields[field]['unit'] = field_dict.get('unit', None)
            fields[field]['ignore'] = to_bool(field_dict.get('ignore', ignore))
            fields[field]['publish_none_value'] = to_bool(field_dict.get('publish_none_value', publish_none_value))
            fields[field]['append_unit_label'] = to_bool(field_dict.get('append_unit_label', append_unit_label))
            fields[field]['conversion_type'] = field_dict.get('conversion_type', conversion_type)
            fields[field]['format_string'] = field_dict.get('format_string', format_string)

        #logdbg("Configured fields: %s" % fields)
        return fields

    def configure_topics(self, service_dict):
        """ Configure the topics. """
        # pylint: disable=too-many-locals, too-many-statements
        topics_dict = service_dict.get('topics', None)
        if topics_dict is None:
            raise ValueError("[[topics]] is required.")

        default_qos = to_int(service_dict.get('qos', 0))
        default_retain = to_bool(service_dict.get('retain', False))
        default_type = service_dict.get('type', 'json')
        default_binding = weeutil.weeutil.option_as_list(service_dict.get('binding', ['archive', 'loop']))

        default_append_label = service_dict.get('append_unit_label', True)
        default_conversion_type = service_dict.get('conversion_type', 'string')
        default_format_string = service_dict.get('format', '%s')

        topics_loop = {}
        topics_archive = {}
        for topic in topics_dict.sections:
            topic_dict = topics_dict.get(topic, {})
            publish = to_bool(topic_dict.get('publish', True))
            qos = to_int(topic_dict.get('qos', default_qos))
            retain = to_bool(topic_dict.get('retain', default_retain))
            data_type = topic_dict.get('type', default_type)
            binding = weeutil.weeutil.option_as_list(topic_dict.get('binding', default_binding))
            unit_system_name = topic_dict.get('unit_system', service_dict.get('unit_system', 'US'))
            unit_system = weewx.units.unit_constants[unit_system_name]

            ignore = to_bool(topic_dict.get('ignore', False))
            publish_none_value = to_bool(topic_dict.get('publish_none_value', False))
            append_unit_label = to_bool(topic_dict.get('append_unit_label', default_append_label))
            conversion_type = topic_dict.get('conversion_type', default_conversion_type)
            format_string = topic_dict.get('format', default_format_string)
            fields_dict = topic_dict.get('fields', None)
            fields = {}
            if fields_dict is not None:
                fields = self.configure_fields(fields_dict, ignore, publish_none_value, append_unit_label, conversion_type, format_string)

            aggregates = topic_dict.get('aggregates', {})
            if aggregates:
                for aggregate in aggregates:
                    if aggregates[aggregate]['period'] not in period_timespan:
                        raise ValueError(f"Invalid 'period', {aggregates[aggregate]['period']}")
                weeutil.config.merge_config(aggregates, self.configure_fields(aggregates,
                                                                              ignore,
                                                                              publish_none_value,
                                                                              append_unit_label,
                                                                              conversion_type,
                                                                              format_string))

            #logdbg("Configured aggregates: %s" % aggregates)

            if 'loop' in binding:
                if not publish:
                    continue
                topics_loop[topic] = {}
                topics_loop[topic]['qos'] = qos
                topics_loop[topic]['retain'] = retain
                topics_loop[topic]['type'] = data_type
                topics_loop[topic]['unit_system'] = unit_system
                topics_loop[topic]['guarantee_delivery'] = to_bool(topic_dict.get('guarantee_delivery', False))
                if topics_loop[topic]['guarantee_delivery'] and topics_loop[topic]['qos'] == 0:
                    raise ValueError("QOS must be greater than 0 to guarantee delivery.")
                topics_loop[topic]['ignore'] = ignore
                topics_loop[topic]['append_unit_label'] = append_unit_label
                topics_loop[topic]['conversion_type'] = conversion_type
                topics_loop[topic]['format'] = format_string
                topics_loop[topic]['fields'] = dict(fields)
                topics_loop[topic]['aggregates'] = dict(aggregates)

            if 'archive' in binding:
                if not publish:
                    continue
                topics_archive[topic] = {}
                topics_archive[topic]['qos'] = qos
                topics_archive[topic]['retain'] = retain
                topics_archive[topic]['type'] = data_type
                topics_archive[topic]['unit_system'] = unit_system
                topics_archive[topic]['guarantee_delivery'] = to_bool(topic_dict.get('guarantee_delivery', False))
                if topics_archive[topic]['guarantee_delivery'] and topics_archive[topic]['qos'] == 0:
                    raise ValueError("QOS must be greater than 0 to guarantee delivery.")
                topics_archive[topic]['ignore'] = ignore
                topics_archive[topic]['append_unit_label'] = append_unit_label
                topics_archive[topic]['conversion_type'] = conversion_type
                topics_archive[topic]['format'] = format_string
                topics_archive[topic]['fields'] = dict(fields)
                topics_archive[topic]['aggregates'] = dict(aggregates)

        logdbg(f"Loop topics: {topics_loop}")
        logdbg(f"Archive topics: {topics_archive}")
        return topics_loop, topics_archive

    def update_record(self, topic_dict, record):
        """ Update the record. """
        final_record = {}
        if topic_dict['unit_system'] is not None:
            updated_record = weewx.units.to_std_system(record, topic_dict['unit_system'])
        for field in updated_record:
            fieldinfo = topic_dict['fields'].get(field, {})
            ignore = fieldinfo.get('ignore', topic_dict.get('ignore'))
            publish_none_value = fieldinfo.get('publish_none_value', topic_dict.get('publish_none_value'))

            if ignore:
                continue
            if updated_record[field] is None and not publish_none_value:
                continue

            (name, value) = self.update_field(topic_dict, fieldinfo, field, updated_record[field], updated_record['usUnits'])
            final_record[name] = value

        for aggregate_observation in topic_dict['aggregates']:
            # logdbg(topic_dict['aggregates'][aggregate_observation])

            time_span = period_timespan[topic_dict['aggregates'][aggregate_observation]['period']](record['dateTime'])

            try:
                aggregate_value_tuple = weewx.xtypes.get_aggregate(topic_dict['aggregates'][aggregate_observation]['observation'],
                                                                time_span, topic_dict['aggregates'][aggregate_observation]['aggregation'],
                                                                self.db_manager)
                aggregate_value = weewx.units.convertStd(aggregate_value_tuple, record['usUnits'])[0]
                # ToDo: only do once?
                weewx.units.obs_group_dict[aggregate_observation] = aggregate_value_tuple[2]

                (name, value) = self.update_field(topic_dict, topic_dict['aggregates'][aggregate_observation],
                                        aggregate_observation,
                                        aggregate_value,
                                        updated_record['usUnits'])

                # ToDo: check if observation already in record
                final_record[name] = value

            except (weewx.CannotCalculate, weewx.UnknownAggregation, weewx.UnknownType) as exception:
                logerr(f"Aggregation failed: {exception}")
                logerr(traceback.format_exc())

        return final_record

    @staticmethod
    def update_field(topic_dict, fieldinfo, field, value, unit_system):
        """ Update field. """
        # pylint: disable=too-many-locals
        name = fieldinfo.get('name', field)
        append_unit_label = fieldinfo.get('append_unit_label', topic_dict.get('append_unit_label'))
        if append_unit_label:
            (unit_type, _) = weewx.units.getStandardUnitType(unit_system, name)
            unit_type = PublishWeeWXThread.UNIT_REDUCTIONS.get(unit_type, unit_type)
            if unit_type is not None:
                name = f"{name}_{unit_type}"

        unit = fieldinfo.get('unit', None)
        if unit is not None:
            (from_unit, from_group) = weewx.units.getStandardUnitType(unit_system, field)
            from_tuple = (value, from_unit, from_group)
            converted_value = weewx.units.convert(from_tuple, unit)[0]
        else:
            converted_value = value

        conversion_type = fieldinfo.get('conversion_type', topic_dict.get('conversion_type'))
        format_string = fieldinfo.get('format', topic_dict.get('format'))
        if conversion_type == 'integer':
            formatted_value = to_int(converted_value)
        else:
            formatted_value = format_string % converted_value
            if conversion_type == 'float':
                formatted_value = to_float(formatted_value)

        return name, formatted_value

    def publish_row(self, time_stamp, data, topics):
        """ Publish the data. """
        record = data

        for topic in topics:
            if topics[topic]['type'] == 'json':
                updated_record = self.update_record(topics[topic], record)
                self.publisher.publish_message(time_stamp,
                                                  topics[topic]['qos'],
                                                  topics[topic]['retain'],
                                                  topic,
                                                  json.dumps(updated_record))
            if topics[topic]['type'] == 'keyword':
                updated_record = self.update_record(topics[topic], record)
                data_keyword = ', '.join(f"{key}={val}" for (key, val) in updated_record.items())
                self.publisher.publish_message(time_stamp,
                                                  topics[topic]['qos'],
                                                  topics[topic]['retain'],
                                                  topic,
                                                  data_keyword)
            if topics[topic]['type'] == 'individual':
                updated_record = self.update_record(topics[topic], record)
                for key, value in updated_record.items():
                    self.publisher.publish_message(time_stamp,
                                                      topics[topic]['qos'],
                                                      topics[topic]['retain'],
                                                      topic + '/' + key,
                                                      value)

    def run(self):
        self.running = True
        #logdbg("Threadid of PublishWeeWXThread: %s" % gettid())

        # need to instantiate inside thread
        self.publisher = AbstractPublisher.get_publisher(self, self.service_dict)

        while self.running:
            try:
                data2 = self.data_queue.get_nowait()
                time_stamp = data2['time_stamp']
                data_type = data2['type']
                data = data2['data']
                if data_type == 'loop':
                    self.publish_row(time_stamp, data, self.topics_loop)
                elif data_type == 'archive':
                    self.publish_row(time_stamp, data, self.topics_archive)
                else:
                    logerr(f"Unknown data type, {data_type}")
            except Queue.Empty:
                # todo this causes another connection, seems to cause no harm
                # does cause a socket error/disconnect message on the server
                self.publisher.client.loop(timeout=0.1)
                # ToDo - investigate my 'sleep' implementation
                self.threading_event.wait(self.keepalive/4)
                self.threading_event.clear()

        loginf("exited loop")
        loginf("thread shutdown")

if __name__ == "__main__":
    def main():
        """ Run it. """
        min_config_dict = {
            'Station': {
                'altitude': [0, 'foot'],
                'latitude': 0,
                'station_type': 'Simulator',
                'longitude': 0
            },
            'Simulator': {
                'driver': 'weewx.drivers.simulator',
            },
            'Engine': {
                'Services': {}
            }
        }
        engine = weewx.engine.StdEngine(min_config_dict)

        config_dict = {
            'debug': 1,
            'MQTTPublish': {
                'topics': {
                    'test/loop': {
                        'binding': 'loop',
                        'type': 'json'
                    }
                }
            },
            'Logging': {
                'root': {
                    'handlers': ['syslog', 'console']
                },
                'loggers': {
                    'user.mqttpublish': {
                        'level': 'DEBUG'
                    }
                }
            }
        }
        config = configobj.ConfigObj(config_dict)

        setup_logging(True, config_dict)
        mqtt_publish = MQTTPublish(engine, config)

        data_json = ''
        with open('tmp/message.json', encoding='UTF-8') as file_object:
            message = file_object.readline()
            while message:
                data_json += message
                message = file_object.readline()

        data = json.loads(data_json)

        new_loop_packet_event = weewx.Event(weewx.NEW_LOOP_PACKET, packet=data)
        engine.dispatchEvent(new_loop_packet_event)

        mqtt_publish.shutDown()
        return

    main()
