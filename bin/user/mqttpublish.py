"""
Publish to MQTT.
Supports publishing "immediately" on loop or archive creation.
And/Or publishing from an externa/persistent queue.

Configuration:
[MQTTPublish]
    [[PublishQueue]]
        # Whether the service is enabled or not.
        # Valid values: True or False
        # Default is True.
        enable = True

        # The data binding gor the external queue.
        # Default is ext_queue_binding
        data_binding = ext_queue_binding

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

        # Maximum period in seconds allowed between communications with the broker.
        # Default is 60.
        keepalive = 60

        # username for broker authentication.
        # Default is None.
        username = None

        # password for broker authentication.
        # Default is None.
        password = None

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

        # Maximum period in seconds allowed between communications with the broker.
        # Default is 60.
        keepalive = 60

        # username for broker authentication.
        # Default is None.
        username = None

        # password for broker authentication.
        # Default is None.
        password = None

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
# todo - rename table
# todo - add option to keep x amount of records when 'cleaning'
# todo - perform clean automatically, using the option above
# todo - change the dbm to 'Manager'

# need to be python 2 compatible pylint: disable=bad-option-value, raise-missing-from, super-with-arguments
# pylint: enable=bad-option-value
try:
    import queue as Queue
except ImportError:
    import Queue

import argparse
import datetime
import json
import os
import random
import ssl
import threading
import time
import traceback

import configobj

import paho.mqtt.client as mqtt

from weeutil.weeutil import to_bool, to_float, to_int, TimeSpan

import weedb
import weewx
from weewx.engine import StdService

VERSION = "0.2.0-rc01a"

try:
    # Test for new-style weewx logging by trying to import weeutil.logger
    import weeutil.logger
    import logging
    log = logging.getLogger(__name__) # confirm to standards pylint: disable=invalid-name
    def setup_logging(logging_level, config_dict):
        """ Setup logging for running in standalone mode."""
        if logging_level:
            weewx.debug = logging_level

        weeutil.logger.setup('wee_MQTTSS', config_dict)

    def logdbg(name, msg):
        """ Log debug level. """
        log.debug("(%s) %s", name, msg)

    def loginf(name, msg):
        """ Log informational level. """
        log.info("(%s) %s", name, msg)

    def logerr(name, msg):
        """ Log error level. """
        log.error("(%s) %s", name, msg)

except ImportError:
    # Old-style weewx logging
    import syslog
    def setup_logging(logging_level, config_dict): # Need to match signature pylint: disable=unused-argument
        """ Setup logging for running in standalone mode."""
        syslog.openlog('wee_MQTTSS', syslog.LOG_PID | syslog.LOG_CONS)
        if logging_level:
            syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG))
        else:
            syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_INFO))

    def logmsg(level, name, msg):
        """ Log the message at the designated level. """
        # Replace '__name__' with something to identify your application.
        syslog.syslog(level, '__name__: %s: (%s)' % (name, msg))

    def logdbg(name, msg):
        """ Log debug level. """
        logmsg(syslog.LOG_DEBUG, name, msg)

    def loginf(name, msg):
        """ Log informational level. """
        logmsg(syslog.LOG_INFO, name, msg)

    def logerr(name, msg):
        """ Log error level. """
        logmsg(syslog.LOG_ERR, name, msg)

schema = [ # confirm to standards pylint: disable=invalid-name
    ('dateTime', 'INTEGER NOT NULL'),
    ('usUnits', 'INTEGER'),
    ('interval', 'INTEGER'),
    ('mid', 'INTEGER'),
    ('rc', 'INTEGER'),
    ('prevMid', 'INTEGER'),
    ('proc_dateTime', 'INTEGER'),
    ('pub_dateTime', 'INTEGER'),
    ('qos', 'INTEGER'),
    ('topic', 'STRING'),
    ('data', 'STRING'),
    ]

# need to rethink
period_timespan = {
    'hour': lambda time_stamp: weeutil.weeutil.archiveHoursAgoSpan(time_stamp),
    'day': lambda time_stamp: weeutil.weeutil.archiveDaySpan(time_stamp),
    'yesterday': lambda time_stamp: weeutil.weeutil.archiveDaySpan(time_stamp, 1, 1),
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

def gettid():
    """Get TID as displayed by htop.
       This is architecture dependent."""
    import ctypes #  need to be python 2 compatible, Want to keep this piece of code self contained. pylint: disable=bad-option-value, import-outside-toplevel
    # pylint: enable=bad-option-value
    libc = 'libc.so.6'
    for cmd in (186, 224, 178):
        tid = ctypes.CDLL(libc).syscall(cmd)
        if tid != -1:
            return tid

    return 0

class MQTTPublish(object):
    """ Managing publishing to MQTT. """
    def __init__(self, publisher, publish_type, db_binder, service_dict):
        self.connected = False
        self.mids = {}
        self.mqtt_logger = {
            mqtt.MQTT_LOG_INFO: loginf,
            mqtt.MQTT_LOG_NOTICE: loginf,
            mqtt.MQTT_LOG_WARNING: loginf,
            mqtt.MQTT_LOG_ERR: logerr,
            mqtt.MQTT_LOG_DEBUG: logdbg
            }

        self.publisher = publisher
        self.publish_type = publish_type

        self.max_retries = to_int(service_dict.get('max_retries', 5))
        self.retry_wait = to_int(service_dict.get('retry_wait', 5))
        mqtt_binding = service_dict.get('mqtt_data_binding', 'mqtt_queue_binding')
        log_mqtt = to_bool(service_dict.get('log', False))
        self.host = service_dict.get('host', 'localhost')
        self.keepalive = to_int(service_dict.get('keepalive', 60))
        self.port = to_int(service_dict.get('port', 1883))
        username = service_dict.get('username', None)
        password = service_dict.get('password', None)
        clientid = service_dict.get('clientid', 'MQTTPublish-' + str(random.randint(1000, 9999)))

        loginf(self.publish_type, "host is %s" % self.host)
        loginf(self.publish_type, "port is %s" % self.port)
        loginf(self.publish_type, "keepalive is %s" % self.keepalive)
        loginf(self.publish_type, "username is %s" % username)
        if password is not None:
            loginf(self.publish_type, "password is set")
        else:
            loginf(self.publish_type, "password is not set")
            loginf(self.publish_type, "clientid is %s" % clientid)

        self.client = mqtt.Client(clientid)

        if log_mqtt:
            self.client.on_log = self.on_log

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

        if username is not None and password is not None:
            self.client.username_pw_set(username, password)

        tls_dict = service_dict.get('tls')
        if tls_dict:
            self.config_tls(tls_dict)

        self._connect()

        self.mqtt_dbm = db_binder.get_manager(data_binding=mqtt_binding, initialize=True)
        self.mqtt_dbm.getSql("PRAGMA journal_mode=WAL;")

    def _connect(self):
        try:
            self.client.connect(self.host, self.port, self.keepalive)
        except Exception as exception: # (want to catch all) pylint: disable=broad-except
            logerr(self.publish_type, "MQTT connect failed with %s and reason %s." % (type(exception), exception))
            logerr(self.publish_type, "%s" % traceback.format_exc())
        retries = 0
        # loop seems to break before connect, perhaps due to logging
        self.client.loop(timeout=0.1)
        time.sleep(1)
        while not self.connected:
            logdbg(self.publish_type, "waiting")
            # loop seems to break before connect, perhaps due to logging
            self.client.loop(timeout=0.1)
            time.sleep(5)

            retries += 1
            if retries > self.max_retries:
                # self.shut_down()
                # Shut thread down, a bit of a hack
                self.publisher.running = False
                return

            try:
                self.client.connect(self.host, self.port, self.keepalive)
            except Exception as exception: # (want to catch all) pylint: disable=broad-except
                logerr(self.publish_type, "MQTT connect failed with %s and reason %s." % (type(exception), exception))
                logerr(self.publish_type, "%s" % traceback.format_exc())

    def _reconnect(self):
        logdbg(self.publish_type, "*** Before reconnect ***")
        self.client.reconnect()
        logdbg(self.publish_type, "*** After reconnect ***")
        retries = 0
        logdbg(self.publish_type, "*** Before loop ***")
        self.client.loop(timeout=1.0)
        logdbg(self.publish_type, "*** After loop ***")
        while not self.connected:
            logdbg(self.publish_type, "waiting")
            self.client.loop(timeout=5.0)

            retries += 1
            if retries > self.max_retries:
                # self.shut_down()
                # Shut thread down, a bit of a hack
                self.publisher.running = False
                return

            self.client.reconnect()

        loginf(self.publish_type, "reconnected")

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
            raise ValueError("Invalid 'certs_required'., %s" % tls_dict['certs_required'])

        tls_version = valid_tls_versions.get(tls_dict.get('tls_version', 'tlsv12'))
        if tls_version is None:
            raise ValueError("Invalid 'tls_version'., %s" % tls_dict['tls_version'])

        self.client.tls_set(ca_certs=ca_certs,
                            certfile=tls_dict.get('certfile'),
                            keyfile=tls_dict.get('keyfile'),
                            cert_reqs=valid_cert_reqs,
                            tls_version=tls_version,
                            ciphers=tls_dict.get('ciphers'))

    def on_connect(self, client, userdata, flags, rc):  # (match callback signature) pylint: disable=unused-argument
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
        loginf(self.publish_type, "Connected with result code %i, %s" %(rc, mqtt.error_string(rc)))
        loginf(self.publish_type, "Connected flags %s" %str(flags))
        self.connected = True

    def on_disconnect(self, client, userdata, rc):  # (match callback signature) pylint: disable=unused-argument
        """ The on_connect callback. """
        # https://pypi.org/project/paho-mqtt/#on-discconnect
        # The rc parameter indicates the disconnection state.
        # If MQTT_ERR_SUCCESS (0), the callback was called in response to a disconnect() call.
        # If any other value the disconnection was unexpected,
        # such as might be caused by a network error.
        loginf(self.publish_type, "Disconnected with result code %i, %s" %(rc, mqtt.error_string(rc)))

        # As of 1.6.1, Paho MQTT cannot have a callback invoke a second callback. So we won't attempt to reconnect here.
        # Because that would cause the on_connect callback to be called. Instead we will just mark as not connected.
        # And check the flag before attempting to publish.
        self.connected = False

    def on_publish(self, client, userdata, mid):  # (match callback signature) pylint: disable=unused-argument
        """ The on_publish callback. """
        time_stamp = "          "
        qos = ""
        guarantee_delivery = False
        if mid in self.mids:
            time_stamp = self.mids[mid]['time_stamp']
            qos = self.mids[mid]['qos']
            guarantee_delivery = self.mids[mid]['guarantee_delivery']
            del self.mids[mid]
        logdbg(self.publish_type, "Published  (%s): %s %s %s" % (int(time.time()), time_stamp, mid, qos))
        logdbg(self.publish_type, "Inflight   (%s): %s" % (int(time.time()), self.mids))
        if guarantee_delivery:
            self.mqtt_dbm.getSql( \
                "UPDATE archive SET pub_dateTime = ? WHERE dateTime == ? and mid == ? and pub_dateTime is NULL;",
                [time.time(), time_stamp, mid])

    def on_log(self, client, userdata, level, msg): # (match callback signature) pylint: disable=unused-argument
        """ The on_log callback. """
        self.mqtt_logger[level](self.publish_type, "MQTT log: %s" %msg)

    def shut_down(self):
        """ Shutting down. """
        try:
            self.mqtt_dbm.close()
        except Exception as exception: # pylint: disable=broad-except
            logerr(self.publish_type, "Close queue dbm failed %s" % exception)
            logerr(self.publish_type, traceback.format_exc())

    def cleanup(self):
        """ Delete messages that were published on the first try.
            Messages that had to be republished are left for root cause analysis. """
        self.mqtt_dbm.getSql("delete from archive where pub_dateTime > 0 and prevMid == 0;")

    def deep_clean(self):
        """ Delete messages that have been published. """
        self.mqtt_dbm.getSql("delete from archive where pub_dateTime is not null;")

    def publish_message(self, time_stamp, prev_mid, guarantee_delivery, qos, retain, topic, data):
        """ Publish the message. """
        # pylint: disable=too-many-arguments
        if not self.connected:
            self._reconnect()
        mqtt_message_info = self.client.publish(topic, data, qos=qos, retain=retain)
        logdbg(self.publish_type, "Publishing (%s): %s %s %s %s" % (int(time.time()), int(time_stamp), mqtt_message_info.mid, qos, topic))
        if guarantee_delivery:
            self.mids[mqtt_message_info.mid] = {}
            self.mids[mqtt_message_info.mid]['time_stamp'] = time_stamp
            self.mids[mqtt_message_info.mid]['qos'] = qos
            self.mids[mqtt_message_info.mid]['guarantee_delivery'] = guarantee_delivery
            self.mqtt_dbm.getSql( \
                "INSERT INTO archive (dateTime, prevMid, proc_dateTime, mid, rc, qos, topic, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                [time_stamp, prev_mid, time.time(), mqtt_message_info.mid, mqtt_message_info.rc, qos, topic, data])

        self.client.loop(timeout=0.1)

    def wait_for_inflight_messages(self):
        """ Wait for acknowledgement that messages have been published. """
        # to do configure
        wait_count = 5
        counter = 0
        sleepy = 2
        while len(self.mids) > 0 and counter < wait_count:
            logdbg(self.publish_type, "() %s in flight messages; on %s loop count %s" %(len(self.mids), counter, self.mids))
            self.client.loop(timeout=0.1)
            time.sleep(sleepy)
            counter += 1

    def republish_message(self):
        """ Republish failed messages."""
        row_count, = self.mqtt_dbm.getSql("SELECT COUNT(*) from archive where pub_dateTime is null;")
        # ToDo - configurable?
        while row_count > 0:
            rows = list(self.mqtt_dbm.genSql(
                "SELECT dateTime, mid, qos, topic, data FROM archive where pub_dateTime is null ORDER BY dateTime  ASC;"))

            i = 1
            for row in rows:
                time_stamp, mid, qos, topic, data = row
                # When republishing failed messages, we will not set the retain flag.
                self.publish_message(time_stamp, mid, True, qos, False, topic, data)
                self.mqtt_dbm.getSql("UPDATE archive SET pub_dateTime = 0 WHERE dateTime = ? and mid == ?;", [time_stamp, mid])

                i += 1
                logdbg(self.publish_type, "republish %i  of %i" % (i, len(rows)))

            self.wait_for_inflight_messages()

            row_count, = self.mqtt_dbm.getSql("SELECT COUNT(*) from archive where pub_dateTime is null;")

    def cancel_message(self):
        """ Cancel messages that are inflight. """
        # todo - configurable
        max_time = time.time() - 24 * 60 * 60
        self.mqtt_dbm.getSql("Delete from archive where pub_dateTime is null and proc_dateTime  < ?", [max_time])

class PublishWeeWX(StdService):
    """ A service to publish WeeWX loop and/or archive data to MQTT. """
    def __init__(self, engine, config_dict):
        self.config_dict = config_dict
        super(PublishWeeWX, self).__init__(engine, self.config_dict)
        self.publish_type = 'WeeWX'

        service_dict = config_dict.get('MQTTPublish', {}).get('PublishWeeWX', {})

        self.enable = to_bool(service_dict.get('enable', True))
        if not self.enable:
            loginf(self.publish_type, "Not enabled, exiting.")
            return

        # todo - make configurable
        self.kill_weewx = []
        self.max_thread_restarts = 2
        self.thread_restarts = 0

        # todo, tie this into the topic bindings somehow...
        binding = weeutil.weeutil.option_as_list(service_dict.get('binding', ['archive', 'loop']))

        self.data_queue = Queue.Queue()

        if 'loop' in binding:
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)

        if 'archive' in binding:
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        self._thread = PublishWeeWXThread(self.config_dict, self.data_queue)
        self.thread_start()

        logdbg(self.publish_type, "Threadid of PublishWeeWX is: %s" % gettid())

    def thread_start(self):
        loginf(self.publish_type, "starting thread")
        self._thread.start()
        # ToDo - configure how long to wait for thread to start
        self.thread_start_wait = 5.0
        loginf(self.publish_type, "joining thread")
        #self._thread.join(self.thread_start_wait)
        loginf(self.publish_type, "joined thread")

        if not self._thread.is_alive():
            loginf(self.publish_type, "oh no")
            raise weewx.WakeupError("Unable to start MQTT publishing thread.")

        loginf(self.publish_type, "started thread")

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
                self._thread = PublishWeeWXThread(self.config_dict, self.data_queue)
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
        loginf(self.publish_type, "SHUTDOWN - initiated")
        if self._thread:
            loginf(self.publish_type, "SHUTDOWN - thread initiated")
            self._thread.running = False
            self._thread.threading_event.set()
            self._thread.join(20.0)
            if self._thread.is_alive():
                logerr(self.publish_type, "Unable to shut down %s thread" %self._thread.name)

            self._thread = None

class PublishQueue(StdService):
    """ A service to publish an external/persistent queue to MQTT. """
    def __init__(self, engine, config_dict):
        super(PublishQueue, self).__init__(engine, config_dict)
        self.publish_type = 'Queue'

        service_dict = config_dict.get('MQTTPublish', {}).get('PublishQueue', {})

        self.enable = to_bool(service_dict.get('enable', True))
        if not self.enable:
            loginf(self.publish_type, "Not enabled, exiting.")
            return

        self._thread = PublishQueueThread(config_dict)
        self.thread_start()

        logdbg(self.publish_type, "Threadid of PublishQueue is: %s" % gettid())

        # Todo, check that thread is alive
        # not sure what event to use, probably new_archive_record

    def thread_start(self):
        self._thread.start()
        # ToDo - configure how long to wait for thread to start
        self.thread_start_wait = 10.0
        self._thread.join(self.thread_start_wait)

        if not self._thread.is_alive():
            loginf(self.publish_type, "oh no")
            raise weewx.WakeupError("Unable to start MQTT publishing thread.")
        #loginf(self.publish_type, run_time)

    def shutDown(self): # need to override parent - pylint: disable=invalid-name
        """Run when an engine shutdown is requested."""
        loginf(self.publish_type, "SHUTDOWN - initiated")
        if self._thread:
            loginf(self.publish_type, "SHUTDOWN - thread initiated")
            self._thread.running = False
            self._thread.threading_event.set()
            self._thread.join(20.0)
            if self._thread.is_alive():
                logerr(self.publish_type, "Unable to shut down %s thread" % self._thread.name)

                self._thread = None

class AbstractPublishThread(threading.Thread):
    """ Some base functionality for publishing. """
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
    def __init__(self, publish_type):
        threading.Thread.__init__(self)

        self.mqtt_publish = None
        self.running = False

        self.publish_type = publish_type

        self.db_manager = None

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

        logdbg(self.publish_type, fields)
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
                        raise ValueError("Invalid 'period', %s" % aggregates[aggregate]['period'])
                weeutil.config.merge_config(aggregates, self.configure_fields(aggregates,
                                                                              ignore,
                                                                              publish_none_value,
                                                                              append_unit_label,
                                                                              conversion_type,
                                                                              format_string))

            logdbg(self.publish_type, aggregates)

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

        logdbg(self.publish_type, topics_loop)
        logdbg(self.publish_type, topics_archive)
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
            logdbg(self.publish_type, topic_dict['aggregates'][aggregate_observation])

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
                logerr(self.publish_type, "Aggregation failed: %s" % exception)
                logerr(self.publish_type, traceback.format_exc())

        return final_record

    @staticmethod
    def update_field(topic_dict, fieldinfo, field, value, unit_system):
        """ Update field. """
        # pylint: disable=too-many-locals
        name = fieldinfo.get('name', field)
        append_unit_label = fieldinfo.get('append_unit_label', topic_dict.get('append_unit_label'))
        if append_unit_label:
            (unit_type, _) = weewx.units.getStandardUnitType(unit_system, name)
            unit_type = AbstractPublishThread.UNIT_REDUCTIONS.get(unit_type, unit_type)
            if unit_type is not None:
                name = "%s_%s" % (name, unit_type)

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
                self.mqtt_publish.publish_message(time_stamp,
                                                  0,
                                                  topics[topic]['guarantee_delivery'],
                                                  topics[topic]['qos'],
                                                  topics[topic]['retain'],
                                                  topic,
                                                  json.dumps(updated_record))
            if topics[topic]['type'] == 'keyword':
                updated_record = self.update_record(topics[topic], record)
                data_keyword = ', '.join("%s=%s" % (key, val) for (key, val) in updated_record.items())
                self.mqtt_publish.publish_message(time_stamp,
                                                  0,
                                                  topics[topic]['guarantee_delivery'],
                                                  topics[topic]['qos'],
                                                  topics[topic]['retain'],
                                                  topic,
                                                  data_keyword)
            if topics[topic]['type'] == 'individual':
                updated_record = self.update_record(topics[topic], record)
                for key in updated_record:
                    self.mqtt_publish.publish_message(time_stamp,
                                                      0,
                                                      topics[topic]['guarantee_delivery'],
                                                      topics[topic]['qos'],
                                                      topics[topic]['retain'],
                                                      topic + '/' + key,
                                                      updated_record[key])

class PublishQueueThread(AbstractPublishThread):
    """ Publish to MQTT from an external/persistent queue. """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, config_dict):
        super(PublishQueueThread, self).__init__('Queue')
        self.config_dict = config_dict
        self.service_dict = config_dict.get('MQTTPublish', {}).get('PublishQueue', {})

        exclude_keys = ['password']
        sanitized_service_dict = {k: self.service_dict[k] for k in set(list(self.service_dict.keys())) - set(exclude_keys)}
        logdbg(self.publish_type, "sanitized configuration removed %s" % exclude_keys)
        logdbg(self.publish_type, "sanitized_service_dict is %s" % sanitized_service_dict)

        self.binding = self.service_dict.get('data_binding', 'ext_queue_binding')
        self.mqtt_binding = self.service_dict.get('mqtt_data_binding', 'mqtt_queue_binding')

        self.catchup_count = int(self.service_dict.get('catchup_count', 10))

        self.keepalive = to_int(self.service_dict.get('keepalive', 60))
        self.wait_before_retry = float(self.service_dict.get('wait_before_retry', 2))
        self.publish_interval = int(self.service_dict.get('publish_interval', 0))
        self.publish_delay = int(self.service_dict.get('publish_delay', 0))

        loginf(self.publish_type, "External queue data binding is  %s" % self.binding)
        loginf(self.publish_type, "MQTT queue data binding is  %s" % self.mqtt_binding)
        loginf(self.publish_type, "Wait before retry is %i" % self.wait_before_retry)
        loginf(self.publish_type, "Publish interval is %i" % self.publish_interval)
        loginf(self.publish_type, "Publish delay is %i" % self.publish_delay)

        self.topics_loop, self.topics_archive = self.configure_topics(self.service_dict)

        self.mids = {}
        self.threading_event = threading.Event()

        self.db_binder = weewx.manager.DBBinder(config_dict)

        self.dbm = None

    def run(self):
        self.running = True
        logdbg(self.publish_type, "Threadid of PublishQueueThread: %s" % gettid())

        self.dbm = self.db_binder.get_manager(data_binding=self.binding)

        # need to instantiate inside thread
        self.mqtt_publish = MQTTPublish(self, 'Queue', self.db_binder, self.service_dict)

        self.catchup()

        while self.running:
            row = self.dbm.getSql("SELECT dateTime, data, dataType FROM archive ORDER BY dateTime  ASC LIMIT 1;")
            if row:
                time_stamp, data, data_type = row
                self.run_sql("Delete from archive where dateTime == ?", [time_stamp])
                if data_type == 'loop':
                    self.publish_row(time_stamp, json.loads(data), self.topics_loop)
                elif data_type == 'archive':
                    self.publish_row(time_stamp, json.loads(data), self.topics_archive)
                else:
                    logerr(self.publish_type, "Unknown data type, %s" % data_type)
            else:
                if self.publish_interval:
                    archive_start = weeutil.weeutil.startOfInterval(time.time(), self.publish_interval)
                    archive_end = archive_start + self.publish_interval
                    time_sleep = archive_end - time.time() + self.publish_delay
                else:
                    time_sleep = self.wait_before_retry
                # ensures that pub/sub messages and mqtt keepalive traffic is maintained with broker.
                if time_sleep > self.keepalive:
                    time_sleep = self.keepalive/4

                logdbg(self.publish_type, "Sleeping   (%s): %s" %(int(time.time()), time_sleep))
                self.threading_event.wait(time_sleep)
                self.threading_event.clear()
                self.mqtt_publish.client.loop(timeout=0.1)

        loginf(self.publish_type, "exited loop")
        self.mqtt_publish.wait_for_inflight_messages()
        self.mqtt_publish.shut_down()

        try:
            self.dbm.close()
        except Exception as exception: # pylint: disable=broad-except
            logerr(self.publish_type, "Close queue dbm failed %s" % exception)
            logerr(self.publish_type, traceback.format_exc())

        self.db_binder.close()

        loginf(self.publish_type, "thread shutdown")

    def catchup(self):
        """ Catchup by processing the external queue. """
        row_count, = self.dbm.getSql("SELECT COUNT(*) from archive;")

        while row_count > self.catchup_count:
            rows = list(self.dbm.genSql("SELECT dateTime, dataType, data FROM archive ORDER BY dateTime  ASC;"))

            i = 1
            for row in rows:
                time_stamp, data_type, data = row
                self.run_sql("Delete from archive where dateTime == ?", [time_stamp])
                if data_type == 'loop':
                    self.publish_row(time_stamp, json.loads(data), self.topics_loop)
                if data_type == 'archive':
                    self.publish_row(time_stamp, json.loads(data), self.topics_archive)
                i += 1
                logdbg(self.publish_type, "catchup %i of %i" % (i, len(rows)))

            row_count, = self.dbm.getSql("SELECT COUNT(*) from archive;")

    def run_sql(self, sql, variables):
        """ Run the SQL and deal with locks. """
        try:
            self.dbm.getSql(sql, variables)
        except weedb.OperationalError as exception:
            logerr(self.publish_type, exception)
            msg = str(exception).lower()
            if msg.startswith("database is locked"):
                pass
            else:
                logerr(self.publish_type, exception)
                raise exception

class PublishWeeWXThread(AbstractPublishThread):
    """Publish WeeWX data to MQTT. """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, config_dict, data_queue):
        super(PublishWeeWXThread, self).__init__('WeeWX')
        self.config_dict = config_dict
        self.service_dict = config_dict.get('MQTTPublish', {}).get('PublishWeeWX', {})

        exclude_keys = ['password']
        sanitized_service_dict = {k: self.service_dict[k] for k in set(list(self.service_dict.keys())) - set(exclude_keys)}
        logdbg(self.publish_type, "sanitized configuration removed %s" % exclude_keys)
        logdbg(self.publish_type, "sanitized_service_dict is %s" % sanitized_service_dict)

        self.db_binder = weewx.manager.DBBinder(config_dict)

        self.topics_loop, self.topics_archive = self.configure_topics(self.service_dict)
        self.wait_before_retry = float(self.service_dict.get('wait_before_retry', 2))
        self.keepalive = to_int(self.service_dict.get('keepalive', 60))


        loginf(self.publish_type, "Wait before retry is %i" % self.wait_before_retry)

        self.data_queue = data_queue
        self.threading_event = threading.Event()

    def run(self):
        self.running = True
        logdbg(self.publish_type, "Threadid of PublishWeeWXThread: %s" % gettid())

        self.db_manager = self.db_binder.get_manager()

        # need to instantiate inside thread
        self.mqtt_publish = MQTTPublish(self, 'WeeWX', self.db_binder, self.service_dict)

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
                    logerr(self.publish_type, "Unknown data type, %s" % data_type)
            except Queue.Empty:
                # todo this causes another connection, seems to cause no harm
                # does cause a socket error/disconnect message on the server
                self.mqtt_publish.client.loop(timeout=0.1)
                # ToDo - investigate my 'sleep' implementation
                self.threading_event.wait(self.keepalive/4)
                self.threading_event.clear()

        loginf(self.publish_type, "exited loop")
        self.mqtt_publish.wait_for_inflight_messages()
        self.mqtt_publish.shut_down()

        self.db_binder.close()

        loginf(self.publish_type, "thread shutdown")

# Example invocations. Paths may vary.
# setup.py install:
# PYTHONPATH=/home/weewx/bin python /home/weewx/bin/user/MQTTSubscribe.py
#
# rpm or deb package install:
# PYTHONPATH=/usr/share/weewx python /usr/share/weewx/user/MQTTSubscribe.py
if __name__ == "__main__":
    def main():
        """ Run it. """
        usage = ""
        parser = argparse.ArgumentParser(usage=usage)
        parser.add_argument("--verbose", action="store_true", dest="verbose",
                            help="Log extra output (debug=1).")
        parser.add_argument("--clean", action="store_true", dest="clean",
                            help="Clean up processed messages")
        parser.add_argument("--deep-clean", action="store_true", dest="deep_clean",
                            help="Perform a deep cleanup up processed messages")
        parser.add_argument("--republish", action="store_true", dest="republish",
                            help="Republish failed messages")
        parser.add_argument("--cancel", action="store_true", dest="cancel",
                            help="Cancel failed messages")
        parser.add_argument("--publish", action="store_true", dest="publish",
                            help="Publish messages")
        parser.add_argument("config_file")

        options = parser.parse_args()

        config_path = os.path.abspath(options.config_file)
        config_dict = configobj.ConfigObj(config_path, file_error=True)
        setup_logging(options.verbose, config_dict)

        db_binder = weewx.manager.DBBinder(config_dict)

        service_dict = config_dict.get('PublishQueue', {})
        mqtt_publish = MQTTPublish(None, '     ', db_binder, service_dict)
        if options.clean:
            mqtt_publish.cleanup()

        if options.deep_clean:
            mqtt_publish.deep_clean()

        if options.cancel:
            mqtt_publish.cancel_message()

        if options.republish:
            mqtt_publish.republish_message()

        if options.publish:
            thread = PublishQueueThread(config_dict)
            thread.run()

    main()
