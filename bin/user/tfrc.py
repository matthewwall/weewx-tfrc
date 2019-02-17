#!/usr/bin/env python
# This driver is a modified version of the weewx-sdr driver.
# See: https://github.com/matthewwall/weewx-sdr
# Copyright 2019 Matthew Wall, Luc Heijst
# Distributed under the terms of the GNU Public License (GPLv3)
"""
Collect data from tfrec.  
see: https://github.com/baycom/tfrec

Run tfrec on a thread and push the output onto a queue.

The SDR detects many different sensors and sensor types, so this driver
includes a mechanism to filter the incoming data, and to map the filtered
data onto the weewx database schema and identify the type of data from each
sensor.

Sensors are filtered based on a tuple that identifies uniquely each sensor.
A tuple consists of the observation name, a unique identifier for the hardware,
and the packet type, separated by periods:

  <observation_name>.<hardware_id>.<packet_type>

The filter and data types are specified in a sensor_map stanza in the driver
stanza.  For example:

[TFRC]
    driver = user.tfrc
    [[sensor_map]]
        temp3 = temperature.25A6.TFA_1Packet
        temp4 = temperature.24A4.TFA_1Packet

If no sensor_map is specified, no data will be collected.

The deltas stanza indicates which observations are cumulative measures and
how they should be split into delta measures.

[TFRC]
    ...
    [[deltas]]
        rain = rain_total

In this case, the value for rain will be a delta calculated from sequential
rain_total observations.

To identify sensors, run the driver directly.  Alternatively, use the options
log_unknown_sensors and log_unmapped_sensors to see data from the TFRC that are
not yet recognized by your configuration.

[TFRC]
    driver = user.tfrc
    log_unknown_sensors = True
    log_unmapped_sensors = True

The default for each of these is False.

"""
from __future__ import with_statement
from subprocess import check_output
import signal
from calendar import timegm
import Queue
import fnmatch
import os
import re
import subprocess
import syslog
import threading
import time
import weewx.drivers
import weewx.units
from weeutil.weeutil import tobool


DRIVER_NAME = 'TFRC'
DRIVER_VERSION = '0.2'

if weewx.__version__ < "3":
    raise weewx.UnsupportedFeature("weewx 3 is required, found %s" %
                                   weewx.__version__)

schema = [('dateTime',             'INTEGER NOT NULL UNIQUE PRIMARY KEY'),
          ('usUnits',              'INTEGER NOT NULL'),
          ('interval',             'INTEGER NOT NULL'),
          ('temp1',                'REAL'),
          ('temp2',                'REAL'),
          ('temp3',                'REAL'),
          ('temp4',                'REAL'),
          ('temp5',                'REAL'),
          ('temp6',                'REAL'),
          ('temp7',                'REAL'),
          ('temp8',                'REAL'),
          ('temp9',                'REAL'),
          ('temp10',               'REAL'),
          ('humidity1',            'REAL'),
          ('humidity2',            'REAL'),
          ('humidity3',            'REAL'),
          ('humidity4',            'REAL'),
          ('humidity5',            'REAL'),
          ('humidity6',            'REAL'),
          ('humidity7',            'REAL'),
          ('humidity8',            'REAL'),
          ('humidity9',            'REAL'),
          ('humidity10',           'REAL'),
          ('rssi1',                'REAL'),
          ('rssi2',                'REAL'),
          ('rssi3',                'REAL'),
          ('rssi4',                'REAL'),
          ('rssi5',                'REAL'),
          ('rssi6',                'REAL'),
          ('rssi7',                'REAL'),
          ('rssi8',                'REAL'),
          ('rssi9',                'REAL'),
          ('rssi10',               'REAL'),
          ('batteryStatus1',       'REAL'),
          ('batteryStatus2',       'REAL'),
          ('batteryStatus3',       'REAL'),
          ('batteryStatus4',       'REAL'),
          ('batteryStatus5',       'REAL'),
          ('batteryStatus6',       'REAL'),
          ('batteryStatus7',       'REAL'),
          ('batteryStatus8',       'REAL'),
          ('batteryStatus9',       'REAL'),
          ('batteryStatus10',      'REAL')
    ]

# tfrec Usage
#
# For 868MHz the usually supplied 10cm DVB-T antenna stick is well suited. Make 
# sure that it is placed on a small metallic ground plane. Avoid near WLAN, DECT 
# or other RF stuff that may disturb the SDR stick.
# 
# To reduce other disturbances, place a sensor in about 2m distance for the first 
# test.
# 
# By default, three sensor types (TFA_1/2/3) are enabled (see -T option for enabling 
# TX22 and WeatherHub).
# 
# Run "tfrec -D". This uses default values and should print a hexdump of received 
# telegrams and the decoded values like this (KlimaLogg Pro sensor):
# 
# #000 1485215350 2d d4 65 b0 86 20 23 60 e0 56 97 ID 65b0 +22.0 35% seq e 
# lowbat 0 RSSI 81
# 
# This message is sensor 65b0 (hex), 22degC, 35% relative humidity, battery OK.
# 
# The sequence counts from 0 to f for every message and allows to detect missing 
# messages.
# 
# RSSI is the receiver strength (usually values between 50 and 85). Typically 
# telegrams with RSSI below 55 tend to have errors. Please note that in the default 
# auto gain mode RSSI values among different sensors can not be compared. Use fixed 
# gain for determining weaker sensors.
# 
# Non-KlimaLoggPro sensors (3143,...) have an additional offset value in the debug 
# output. Subtract that from 868250 if you have reception problems (see below).
# 
# If that works, you can omit the -D. Then only successfully decoded data is printed. 
# If you want to store the data, you can use the option -e . For every received 
# message is called with the arguments
# 
# id temp hum seq batfail rssi timestamp
# The timestamp is the typical Unix timestamp of the reception time.
# 
# Start with: 'tfrec -q -e /bin/echo'. Then all output comes from the echo-command.
#
# Other useful options:
# 
# -g : Manual (for 820T-tuner 0...50, -1: auto gain=default)
# -d : Use specified stick (use -d ? to list all sticks)
# -w : Run for timeout seconds and exit
# -m : If 1, store data and only execute one handler for each ID at exit (use with -w)
# -W : Use wider filter (+-80kHz vs +-44kHz), tolerates more frequency offset, but is 
#      less sensitive
# -T : Enable individual sensor types, bitmask ORing the following choices as hex value:
# 1: TFA_1 (KlimaLoggPro)
# 2: TFA_2 (17240bit/s)
# 4: TFA_3 (9600bit/s)
# 8: TX22 (8842bit/s) NOT ENABLED BY DEFAULT!
# 20: WeatherHub (6000bit/s) NOT ENABLED BY DEFAULT!
# Example: "-T 2a" enables TFA_2 (2), TX22 (8) and WeatherHub (20)
# -t: Manually set trigger level (usually 200-1000). If 0, the level is adjusted 
#     automatically (default)
#
# NOTE: the tfrc.py driver use the -D option output; without this option it won't work !!!
DEFAULT_CMD = 'tfrec -D' 

def loader(config_dict, _):
    return TFRCDriver(**config_dict[DRIVER_NAME])

def confeditor_loader():
    return TFRCConfigurationEditor()


def logmsg(level, msg):
    syslog.syslog(level, 'tfrc: %s: %s' %
                  (threading.currentThread().getName(), msg))

def logdbg(msg):
    logmsg(syslog.LOG_DEBUG, msg)

def loginf(msg):
    logmsg(syslog.LOG_INFO, msg)

def logerr(msg):
    logmsg(syslog.LOG_ERR, msg)


class AsyncReader(threading.Thread):

    def __init__(self, fd, queue, label):
        threading.Thread.__init__(self)
        self._fd = fd
        self._queue = queue
        self._running = False
        self.setDaemon(True)
        self.setName(label)

    def run(self):
        logdbg("start async reader for %s" % self.getName())
        self._running = True
        for line in iter(self._fd.readline, ''):
            self._queue.put(line)
            if not self._running:
                break

    def stop_running(self):
        self._running = False


class ProcManager():
    TS = re.compile(' {10}')

    def __init__(self):
        self._cmd = None
        self._process = None
        self.stdout_queue = Queue.Queue()
        self.stdout_reader = None
        self.stderr_queue = Queue.Queue()
        self.stderr_reader = None

    def get_pid(self, name):
        return map(int,check_output(["pidof",name]).split())

    def startup(self, cmd, path=None, ld_library_path=None):
        # kill existiing tfrec processes
        try:
            pid_list = self.get_pid("tfrec")
            for pid in pid_list:
                os.kill(int(pid), signal.SIGKILL)
                loginf("tfrec with pid %s killed" % pid)
        except:
            pass

        self._cmd = cmd
        loginf("startup process '%s'" % self._cmd)
        env = os.environ.copy()
        if path:
            env['PATH'] = path + ':' + env['PATH']
        if ld_library_path:
            env['LD_LIBRARY_PATH'] = ld_library_path
        try:
            self._process = subprocess.Popen(cmd.split(' '),
                                             env=env,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
            self.stdout_reader = AsyncReader(
                self._process.stdout, self.stdout_queue, 'stdout-thread')
            self.stdout_reader.start()
            self.stderr_reader = AsyncReader(
                self._process.stderr, self.stderr_queue, 'stderr-thread')
            self.stderr_reader.start()
        except (OSError, ValueError), e:
            raise weewx.WeeWxIOError("failed to start process: %s" % e)

    def shutdown(self):
        loginf('shutdown process %s' % self._cmd)
        self.stdout_reader.stop_running()
        self.stderr_reader.stop_running()
        # kill existiing tfrec processes
        pid_list = self.get_pid("tfrec")
        for pid in pid_list:
            os.kill(int(pid), signal.SIGKILL)
            loginf("tfrec with pid %s killed" % pid)

    def running(self):
        return self._process.poll() is None

    def get_stderr(self):
        lines = []
        while not self.stderr_queue.empty():
            lines.append(self.stderr_queue.get())
        return lines

    def get_stdout(self):
        lines = []
        # When a lot TFA sensors are read (e.g. 10), a hangup
        # will occur regularly, sometimes of more than a minute.
        # Therefor a maximum run-time of get_stdout of 10 seconds 
        # is invoked to let genLoopPackets process the yielded lines. 
        start_time = int(time.time())
        while self.running() and int(time.time()) - start_time < 10:
            try:
                line = self.stdout_queue.get(True, 3)
                m = ProcManager.TS.search(line)
                if m and lines:
                    yield lines
                    lines = []
                lines.append(line) 
            except Queue.Empty:
                yield lines
                lines = []
        yield lines


class Packet:

    def __init__(self):
        pass

    @staticmethod
    def parse_text(ts, payload, lines):
        return None

    @staticmethod
    def add_identifiers(pkt, sensor_id='', packet_type=''):
        # qualify each field name with details about the sensor.  not every
        # sensor has all three fields.
        # observation.<sensor_id>.<packet_type>
        packet = dict()
        if 'dateTime' in pkt:
            packet['dateTime'] = pkt.pop('dateTime', 0)
        if 'usUnits' in pkt:
            packet['usUnits'] = pkt.pop('usUnits', 0)
        for n in pkt:
            packet["%s.%s.%s" % (n, sensor_id, packet_type)] = pkt[n]
        return packet


class TFA(object):
    @staticmethod
    def insert_ids(pkt, pkt_type):
        # there should be a sensor_id field in the packet to identify sensor.
        # ensure the sensor_id is upper-case - it should be 4 hex characters.
        sensor_id = str(pkt.pop('hardware_id', '0000')).upper()
        return Packet.add_identifiers(pkt, sensor_id, pkt_type)


class TFA_1Packet(Packet):

    IDENTIFIER = "%  seq "
    PATTERN = re.compile('^#\d+ ([\d]+)  .+ID ([0-9a-f]{4}) ([\d.\+-]+) ([\d]+)%  seq ([0-9a-fA-F]+) lowbat ([\d]+) RSSI ([\d]+)')

    @staticmethod
    def parse_text(payload, lines):
        pkt = dict()
        m = TFA_1Packet.PATTERN.search(lines[0])
        if m:
            pkt['dateTime'] = int(m.group(1))
            pkt['usUnits'] = weewx.METRIC
            pkt['hardware_id'] = m.group(2)
            pkt['temperature'] = float(m.group(3))
            if m.group(4) != '0':
                pkt['humidity'] = float(m.group(4))
            ###pkt['sequence'] = m.group(5)
            pkt['lowbat'] = float(m.group(6))
            pkt['rssi'] = float(m.group(7))
            pkt = TFA.insert_ids(pkt, TFA_1Packet.__name__)
        else:
            loginf("TFA_1Packet: unrecognized data: '%s'" % lines[0])
        ###logdbg("TFA_1Packet=%s" % pkt)
        lines.pop(0)
        return pkt

    # format with -D option:
    # #1234 1485215350  2d d4 65 b0 86 20 23 60 e0 56 97           ID 65b0 +22.0 35% seq e lowbat 0 RSSI 81
    # This message is sensor 65b0 (hex), 22degC, 35% relative humidity, battery OK.
    # The sequence counts from 0 to f for every message and allows to detect missing messages.
    # RSSI is the receiver strength (usually values between 50 and 85). Typically telegrams with 
    # RSSI below 55 tend to have errors. Please note that in the default auto gain mode RSSI values
    # among different sensors can not be compared. Use fixed gain for determining weaker sensors.


class TFA_2Packet(Packet):
    # NOT TESTED !!!
    IDENTIFIER = "ID 1000"
    PATTERN = re.compile('^#\d+ ([\d]+)  .+ID 1000([09])([0-9a-f]{2})([0-4]) ([\d.-]+) ([\d]+) ([\d]+) ([\d]+) RSSI ([\d]+) Offset ([\d]+)kHz')

    @staticmethod
    def parse_text(payload, lines):
        pkt = dict()
        m = TFA_2Packet.PATTERN.search(lines[0])
        if m:
            pkt = TFA.insert_ids(pkt, TFA_2Packet.__name__)
        else:
            loginf("TFA_2Packet: unrecognized data: '%s'" % lines[0])
        lines.pop(0)
        return pkt

    # WARNING: Parsing not implemented !
    # format with -D option:
    # #1234 1485215350  2d d4 65 b0 86 20 23 60 e0 56 97           ID 20009900 +17.2 47 12 12 RSSI 83 Offset 11kHz
    # These sensors do not have a unique ID. Each time the battery is inserted, a random ID is 
    # generated. The 2-hex-digit-ID is displayed after powerup as the last number before the 
    # temperature appears. tfrec generates a 8-hex-digit ID with the following scheme:
    # 
    # a000bccd
    # 
    # a = type 1 (17240baud types), 2 (9600 baud types), 3 (TX22)
    # b = static value (0 for TX22, usually 9 for others)
    # cc = random ID
    # d = subtype
    # d = 0 (internal temperature sensor)
    # d = 1 (external temperature sensor for 3143)
    # d = 1 only humidity (TX22)
    # d = 2 rain counter as temperature value (TX22)
    # d = 3 speed as temperature in m/s, direction as humidity (TX22)
    # d = 4 gust speed as temperature in m/s (TX22)
    # The output format for the handler is identical to the other sensors. The sequence counter is 
    # set to 0. If the sensor does not support humidity, it is set to 0.
    # 
    # Please note that the TX22 system can deliver multiple outputs for each of its subsystems. 
    # With -D, only the actually sent values are printed.
    # 
    # The debug output also shows the frequency offset like this:
    # 
    # ID 20009900 +17.2 47 12 12 RSSI 83 Offset 11kHz
    # This indicates that the real frequency is actually 11kHz below the center frequency. 
    # About +-30kHz are tolerated with decreasing sensitivity at the edges. If you have reception 
    # problems, adjust the center frequency with -f.
    # 
    # Note #1: The frequency offset is very inaccurate for real offsets beyond 30kHz and may 
    # indicate even the wrong direction. So better try in 10kHz steps...
    # 
    # Note #2: My 3144 sensor is about 25kHz lower (at 868225kHz), maybe this affects all sensors.


class TFA_3Packet(Packet):
    # NOT TESTED !!!
    IDENTIFIER = "ID 2000"
    PATTERN = re.compile('^#\d+ ([\d]+)  .+ID 2000([09])([0-9a-f]{2})([0-4]) ([\d.-]+) ([\d]+) ([\d]+) ([\d]+) RSSI ([\d]+) Offset ([\d]+)kHz')

    @staticmethod
    def parse_text(payload, lines):
        pkt = dict()
        m = TFA_3Packet.PATTERN.search(lines[0])
        if m:
            pkt = TFA.insert_ids(pkt, TFA_3Packet.__name__)
        else:
            loginf("TFA_3Packet: unrecognized data: '%s'" % lines[0])
        lines.pop(0)
        return pkt

    # WARNING: Parsing not implemented !
    # format
    # See: TFA_2Packet


class TX22Packet(Packet):
    # NOT TESTED !!!
    IDENTIFIER = "ID3000"
    PATTERN = re.compile('^#\d+ ([\d]+)  .+ID 3000([09])([0-9a-f]{2})([0-4]) ([\d.-]+) ([\d]+) ([\d]+) ([\d]+) RSSI ([\d]+) Offset ([\d]+)kHz')

    @staticmethod
    def parse_text(ts, payload, lines):
        pkt = dict()
        m = TX22Packet.PATTERN.search(lines[0])
        if m:
            pkt = TFA.insert_ids(pkt, TX22Packet.__name__)
        else:
            loginf("TX22Packet: unrecognized data: '%s'" % lines[0])
        lines.pop(0)
        return pkt

    # WARNING: Parsing not implemented !
    # format
    # See: TFA_2Packet


class WeatherHubPacket(Packet):
    # NOT TESTED !!!
    IDENTIFIER = " {10}"  # no unique identifiere found
    PATTERN = re.compile('^#\d+ ([\d]+)  .+([0-9a-f]{12})([1-5c-e]) ([\d.-]+) ([\d]+) ([\d]+) ([\d]+) ([\d]+) ([\d]+)')

    @staticmethod
    def parse_text(payload, lines):
        pkt = dict()
        m = WeatherHubPacket.PATTERN.search(lines[0])
        if m:
            pkt = TFA.insert_ids(pkt, WeatherHubPacket.__name__)
        else:
            loginf("WeatherHubPacket: unrecognized data: '%s'" % lines[0])
        lines.pop(0)
        return pkt

    # WARNING: Parsing not implemented !
    # format with -D option: WARNING: format below is a guess
    # #1234 1525996300  2d d4 65 b0 86 20 23 60 e0 56 97        0b3d9ddeeabc3 +0.7 270 950 0 92 0 1525996300
    # These sensors have unique 6 byte IDs, usually printed on sensor. The first byte describes 
    # the sensor type. Sensors values for rain, wind and door states are mapped like TX22 by the 
    # last nibble of the ID at the temperature and humidity values:
    # 
    # IIIIIIIIIIIIT
    # 
    # I: 6 Byte ID, T: Type (4bit)
    # Subtype
    # T=0: temperature, humidity (0 if not available), indoor values for stations with multiple 
    # sensors
    # T=1: External (cable) temperature sensor (WHB type 7)
    # T=2: Rain sensor: tempval=rain-counter, hum=time in s since last pulse
    # T=3: Wind sensor: tempval=speed (m/s), hum=direction (degree)
    # T=4: Wind sensor: tempval=gust speed (m/s)
    # T=5: Door/water sensor: tempval=state (1=open/wet), hum=time in s since last event (only 
    # door sensor)
    # T=0xc to 0xe: temperature/humidity of extra sensors (or stations)
    # Thus, some sensor types deliver two or more output messages in one go, see the following 
    # examples:
    # 
    # Wind sensor with ID 0b3d9ddeeabc:
    # 0b3d9ddeeabc3 +0.7 270 950 0 92 0 1525996300    -> 2=Wind 0.7m/s from west (270deg)
    # 0b3d9ddeeabc4 +5.8 0 950 0 92 0 1525996300      -> 3=Gust 5.8m/s
    #
    # Temperature/Humidity/Wetness sensor with ID 0469fe50dabc:
    # 0469fe50dabc0 +21.3 51 16394 0 90 0 1525979222  -> 0= Temp +21.3, humidity 51%
    # 0469fe50dabc5 +1.0 0 16394 0 90 0 1525979222    -> 5= State=1 (wet)
    # 
    # Rain sensor with ID 0833c2708abc (0.25mm/m^2 rain per count)
    # 0833c2708abc2 +9.0 774 10 0 92 0 1525998514   -> 2= Counter 9, last event 774s ago
    # 0833c2708abc0 +21.0 0 10 0 92 0 1525998514    -> 0= Temp 21.0
    # 
    # Station ID 117addaf2ff6 with one internal and 3 external T/H sensors (TFA 30.3060.01)
    # 117addaf2ff60 +22.0 55 1772 0 0 0 1540944521    -> 0=indoor
    # 117addaf2ff6c +11.6 86 1772 0 0 0 1540944521    -> c=sensor#1
    # 117addaf2ff6d +22.2 51 1772 0 0 0 1540944521    -> d=sensor#2
    # 117addaf2ff6e +18.8 62 1772 0 0 0 1540944521    -> e=sensor#3
    #
    # Some sensors (rain, wind, door) send a history of previous values. This history is 
    # currently just internally decoded but not used. You can see if with the "-DD" option.


class PacketFactory(object):

    # FIXME: do this with class introspection
    KNOWN_PACKETS = [
        TFA_1Packet,  # KlimaLogg Pro sensors
        TFA_2Packet,
        TFA_3Packet,
        TX22Packet,
        WeatherHubPacket
    ]

    @staticmethod
    def create(lines):
        # return a list of packets from the specified lines
        while lines:
            pkt = PacketFactory.parse_text(lines)
            if pkt is not None:
                yield pkt

    @staticmethod
    def parse_text(lines):
        pkt = dict()
        payload = lines[0].strip()
        if payload:
            for parser in PacketFactory.KNOWN_PACKETS:
                if payload.find(parser.IDENTIFIER) >= 0:
                    pkt = parser.parse_text(payload, lines)
                    return pkt
            logdbg("parse_text: unknown format: payload=%s" % payload)
        else:
            logdbg("parse_text failed: lines=%s" % lines)
        lines.pop(0)
        return None


class TFRCConfigurationEditor(weewx.drivers.AbstractConfEditor):
    @property
    def default_stanza(self):
        return """
[TFRC]
    # This section is for the software-defined radio driver.

    # The driver to use
    driver = user.tfrc

    # How to invoke the tfrc command
    cmd = %s

    # The sensor map associates observations with database fields.  Each map
    # element consists of a tuple on the left and a database field name on the
    # right.  The tuple on the left consists of:
    #
    #   <observation_name>.<sensor_identifier>.<packet_type>
    #
    # The sensor_identifier is hardware-specific.  For example, TFA sensors
    # have a 4 character hexadecimal identifier, whereas fine offset sensor
    # clusters have a 4 digit identifier.
    #
    # glob-style pattern matching is supported for the sensor_identifier.
    #
    # map data from any fine offset sensor cluster to database field names
#    [[sensor_map]]
#        outTemp = temperature.*.TFA_1Packet
#        outHumidity = humidity.*.TFA_1Packet
#        windGust = wind_gust.*.TFA_2Packet
#        outBatteryStatus = lowbat.*.TFA_2Packet
#        rain_total = rain_total.*.TFA_2Packet
#        windSpeed = wind_speed.*.TFA_2Packet
#        windDir = wind_dir.*.TFA_2Packet

""" % DEFAULT_CMD


class TFRCDriver(weewx.drivers.AbstractDevice):

    # map the counter total to the counter delta.  for example, the pair
    #   rain:rain_total
    # will result in a delta called 'rain' from the cumulative 'rain_total'.
    # these are applied to mapped packets.
    DEFAULT_DELTAS = {
        'rain': 'rain_total',
        'strikes': 'strikes_total'}

    def __init__(self, **stn_dict):
        loginf('driver version is %s' % DRIVER_VERSION)
        self._log_unknown = tobool(stn_dict.get('log_unknown_sensors', False))
        self._log_unmapped = tobool(stn_dict.get('log_unmapped_sensors', False))
        self._sensor_map = stn_dict.get('sensor_map', {})
        loginf('sensor map is %s' % self._sensor_map)
        self._deltas = stn_dict.get('deltas', TFRCDriver.DEFAULT_DELTAS)
        loginf('deltas is %s' % self._deltas)
        self._counter_values = dict()
        cmd = stn_dict.get('cmd', DEFAULT_CMD)
        path = stn_dict.get('path', None)
        ld_library_path = stn_dict.get('ld_library_path', None)
        self._last_pkt = None # avoid duplicate sequential packets
        self._mgr = ProcManager()
        self._mgr.startup(cmd, path, ld_library_path)

    def closePort(self):
        self._mgr.shutdown()

    @property
    def hardware_name(self):
        return 'TFRC'

    def genLoopPackets(self):
        while self._mgr.running():
            for lines in self._mgr.get_stdout():
                ###logdbg("genLoopPackets lines:%s" % lines)
                for packet in PacketFactory.create(lines):
                    if packet:
                        packet = self.map_to_fields(packet, self._sensor_map)
                        if packet:
                            if packet != self._last_pkt:
                                logdbg("packet=%s" % packet)
                                self._last_pkt = packet
                                self._calculate_deltas(packet)
                                yield packet
                            else:
                                logdbg("ignoring duplicate packet %s" % packet)
                        elif self._log_unmapped:
                            loginf("unmapped: %s (%s)" % (lines, packet))
                    elif self._log_unknown:
                        loginf("missed (unparsed): %s" % lines)
            self._mgr.get_stderr() # flush the stderr queue
        else:
            logerr("err: %s" % self._mgr.get_stderr())
            raise weewx.WeeWxIOError("tfrc process is not running")

    def _calculate_deltas(self, pkt):
        for k in self._deltas:
            label = self._deltas[k]
            if label in pkt:
                pkt[k] = self._calculate_delta(
                    label, pkt[label], self._counter_values.get(label))
                self._counter_values[label] = pkt[label]

    @staticmethod
    def _calculate_delta(label, newtotal, oldtotal):
        delta = None
        if newtotal is not None and oldtotal is not None:
            if newtotal >= oldtotal:
                delta = newtotal - oldtotal
            else:
                loginf("%s decrement ignored:"
                       " new: %s old: %s" % (label, newtotal, oldtotal))
        return delta

    @staticmethod
    def map_to_fields(pkt, sensor_map):
        # selectively get elements from the packet using the specified sensor
        # map.  if the identifier is found, then use its value.  if not, then
        # skip it completely (it is not given a None value).  include the
        # time stamp and unit system only if we actually got data.
        packet = dict()
        for n in sensor_map.keys():
            label = TFRCDriver._find_match(sensor_map[n], pkt.keys())
            if label:
                packet[n] = pkt.get(label)
        if packet:
            for k in ['dateTime', 'usUnits']:
                packet[k] = pkt[k]
        return packet

    @staticmethod
    def _find_match(pattern, keylist):
        # find the first key in pkt that matches the specified pattern.
        # the general form of a pattern is:
        #   <observation_name>.<sensor_id>.<packet_type>
        # do glob-style matching.
        if pattern in keylist:
            return pattern
        match = None
        pparts = pattern.split('.')
        if len(pparts) == 3:
            for k in keylist:
                kparts = k.split('.')
                if (len(kparts) == 3 and
                    TFRCDriver._part_match(pparts[0], kparts[0]) and
                    TFRCDriver._part_match(pparts[1], kparts[1]) and
                    TFRCDriver._part_match(pparts[2], kparts[2])):
                    match = k
                    break
                elif pparts[0] == k:
                    match = k
                    break
        return match

    @staticmethod
    def _part_match(pattern, value):
        # use glob matching for parts of the tuple
        matches = fnmatch.filter([value], pattern)
        return True if matches else False


if __name__ == '__main__':
    import optparse

    usage = """%prog [--debug] [--help] [--version]
        [--action=(show-packets | show-detected | list-supported)]
        [--cmd=RTL_CMD] [--path=PATH] [--ld_library_path=LD_LIBRARY_PATH]

Actions:
  show-packets: display each packet (default)
  show-detected: display a running count of the number of each packet type
  list-supported: show a list of the supported packet types

Hide:
  This is a comma-separate list of the types of data that should not be
  displayed.  Default is to show everything."""

    syslog.openlog('tfrc', syslog.LOG_PID | syslog.LOG_CONS)
    syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_INFO))
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('--version', dest='version', action='store_true',
                      help='display driver version')
    parser.add_option('--debug', dest='debug', action='store_true',
                      help='display diagnostic information while running')
    parser.add_option('--cmd', dest='cmd', default=DEFAULT_CMD,
                      help='tfrc command with options')
    parser.add_option('--path', dest='path',
                      help='value for PATH')
    parser.add_option('--ld_library_path', dest='ld_library_path',
                      help='value for LD_LIBRARY_PATH')
    parser.add_option('--hide', dest='hidden', default='empty',
                      help='output to be hidden: out, parsed, unparsed, empty')
    parser.add_option('--action', dest='action', default='show-packets',
                      help='actions include show-packets, show-detected, list-supported')

    (options, args) = parser.parse_args()

    if options.version:
        print "tfrc driver version %s" % DRIVER_VERSION
        exit(1)

    if options.debug:
        syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG))

    if options.action == 'list-supported':
        for pt in PacketFactory.KNOWN_PACKETS:
            print pt.IDENTIFIER
    elif options.action == 'show-detected':
        # display identifiers for detected sensors
        mgr = ProcManager()
        mgr.startup(options.cmd, path=options.path,
                    ld_library_path=options.ld_library_path)
        detected = dict()
        for lines in mgr.get_stdout():
#            print "out:", lines
            for p in PacketFactory.create(lines):
                if p:
                    del p['usUnits']
                    del p['dateTime']
                    keys = p.keys()
                    label = re.sub(r'^[^\.]+', '', keys[0])
                    if label not in detected:
                        detected[label] = 0
                    detected[label] += 1
                print detected
    else:
        # display output and parsed/unparsed packets
        hidden = [x.strip() for x in options.hidden.split(',')]
        mgr = ProcManager()
        mgr.startup(options.cmd, path=options.path,
                    ld_library_path=options.ld_library_path)
        for lines in mgr.get_stdout():
            if 'out' not in hidden and (
                'empty' not in hidden or len(lines)):
                print "out:", lines
            for p in PacketFactory.create(lines):
                if p:
                    if 'parsed' not in hidden:
                        print 'parsed: %s' % p
                else:
                    if 'unparsed' not in hidden and (
                        'empty' not in hidden or len(lines)):
                        print "unparsed:", lines
        for lines in mgr.get_stderr():
            print "err:", lines
