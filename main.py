import asyncio
import base64
import logging
from collections import deque
from datetime import datetime, timedelta
from threading import Thread

import aiosqlite
import paho.mqtt.client as mqtt
import serial
import sqlite3
from colorlog import ColoredFormatter

USB_PORT = "/dev/ttyUSB0"
DB_PATH = 'sqlite.db'
MQTT_HOST = '192.168.80.197'

MQTT_USERNAME = 'homeassistant'
MQTT_PASSWORD = ''
MQTT_PORT = 1883
MQTT_TIMEOUT = 60

FEND = '7E7E'

fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
colorfmt = "%(log_color)s{}%(reset)s".format(fmt)
datefmt = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers[0].setFormatter(ColoredFormatter(
    colorfmt,
    datefmt=datefmt,
    reset=True,
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red',
    }
))

_LOGGER = logging.getLogger(__name__)


class DataHandler(object):
    def __init__(self):
        self.meter_id = None
        self.effect = deque(maxlen=30)
        self.mqtt_client = None
        self.connected = False
        self.prev_meter_value = None
        self.db_path = DB_PATH
        self._last_mqtt = datetime.now() - timedelta(hours=10)
        self.loop = asyncio.new_event_loop()

        def f(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        t = Thread(target=f, args=(self.loop,))
        t.start()

        if MQTT_HOST:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

            def _on_connect(client, _, flags, return_code):
                self.connected = True

            self.mqtt_client.on_connect = _on_connect
            self.mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, MQTT_TIMEOUT)
            self.mqtt_client.loop_start()

    def add_data(self, txt_buf):
        _LOGGER.info("meter id: %s", self.meter_id)
        txt_buf = txt_buf[34:]
        decoded_data = self.decode(txt_buf)
        effect = decoded_data.get('Effect')
        if effect:
            self.effect.append(effect)
        meter_id = decoded_data.get('Meter-ID')
        if meter_id:
            self.meter_id = meter_id
        _LOGGER.info("Decoded data %s:", decoded_data)

        self.loop.call_soon_threadsafe(asyncio.async, self.send_to_db(decoded_data))
        self.loop.call_soon_threadsafe(asyncio.async, self.send_to_mqtt(decoded_data))

    async def send_to_db(self, data):
        if not self.db_path:
            return
        effect = data.get('Effect')
        if not effect:
            return
        date = data.get('day', '') + data.get('month', '') + data.get('year', '') +\
            data.get('hour', '') + data.get('minute', '') + data.get('second', '')

        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute('''INSERT INTO HANdata(date, effect) VALUES(?, ?)''', (date, effect))
            await cur.close()
            await db.commit()

    async def send_to_mqtt(self, data):
        if not self.connected or not self.meter_id:
            return
        if 'Cumulative_hourly_active_import_energy' in data:
            val = data.get('Cumulative_hourly_active_import_energy')
            prefix = '{}/{}/{}'.format(str(self.meter_id),
                                       'Cumulative_hourly_active_import_energy', 'wh')
            self.mqtt_client.publish(prefix, val, qos=1, retain=True)
            if self.prev_meter_value:
                prefix = '{}/{}/{}'.format(str(self.meter_id),
                                           'Diff_cumulative_hourly_active_import_energy', 'wh')
                self.mqtt_client.publish(prefix, val - self.prev_meter_value, qos=1, retain=True)
            self.prev_meter_value = val
        if 'Cumulative_hourly_reactive_import_energy' in data:
            val = data.get('Cumulative_hourly_reactive_import_energy')
            prefix = '{}/{}/{}'.format(str(self.meter_id),
                                       'Cumulative_hourly_reactive_import_energy', 'wh')
            self.mqtt_client.publish(prefix, val, qos=1, retain=True)
        now = datetime.now()
        if now - self._last_mqtt < timedelta(minutes=1):
            return
        self._last_mqtt = now
        prefix = '{}/{}/{}'.format(str(self.meter_id), 'effect', 'watt')
        val = round(sum(self.effect) / len(self.effect), 1)
        self.mqtt_client.publish(prefix, val, qos=1, retain=True)
        _LOGGER.info("Watt: %s", val)

    @staticmethod
    def decode_date(date_str):
        return {
            'year': int(date_str[4:8], 16),
            'month': int(date_str[8:10], 16),
            'day': int(date_str[10:12], 16),
            'hour': int(date_str[14:16], 16),
            'minute': int(date_str[16:18], 16),
            'second': int(date_str[18:20], 16),
        }

    def decode(self, txt_buf):
        try:
            res = self.decode_date(txt_buf)
            txt_buf = txt_buf[28:]
            if txt_buf[:2] != '02':
                _LOGGER.error("Unknown data %s", txt_buf[:2])
                return {}
            pkt_type = txt_buf[2:4]
            txt_buf = txt_buf[4:]
            if pkt_type == '01':
                res['Effect'] = int(txt_buf[2:10], 16)
            elif pkt_type in ['09', '0E']:
                res['Version identifier'] = base64.b16decode(txt_buf[4:18]).decode("utf-8")
                txt_buf = txt_buf[18:]
                res['Meter-ID'] = base64.b16decode(txt_buf[4:36]).decode("utf-8")
                txt_buf = txt_buf[36:]
                res['Meter type'] = base64.b16decode(txt_buf[4:20]).decode("utf-8")
                txt_buf = txt_buf[20:]
                res['Effect'] = int(txt_buf[2:10], 16)
                if pkt_type == '0E':
                    txt_buf = txt_buf[10:]
                    txt_buf = txt_buf[78:]
                    res['Cumulative_hourly_active_import_energy'] = int(txt_buf[2:10], 16)
                    txt_buf = txt_buf[10:]
                    res['Cumulative_hourly_active_export_energy'] = int(txt_buf[2:10], 16)
                    txt_buf = txt_buf[10:]
                    res['Cumulative_hourly_reactive_import_energy'] = int(txt_buf[2:10], 16)
                    txt_buf = txt_buf[10:]
                    res['Cumulative_hourly_reactive_export_energy'] = int(txt_buf[2:10], 16)
            else:
                _LOGGER.warning("Unknown type %s", txt_buf[2:4])
                return {}
        except ValueError:
            return {}
        return res


def create_db():
    """Make the database"""
    loop = asyncio.get_event_loop()

    async def _execute():
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute('''CREATE TABLE "HANdata" (
                                   `id`    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                                   'date'	STRING,
                                   'effect' REAL
                                   )''')

            await cur.close()

    loop.run_until_complete(_execute())


def run():
    txt_buf = ''
    ser = serial.Serial(USB_PORT, baudrate=2400, timeout=0, parity=serial.PARITY_NONE)
    data_handler = DataHandler()
    while True:
        if ser.inWaiting():
            txt_buf += "".join("{0:02x}".format(x).upper() for x in bytearray(ser.read(200)))

            if len(txt_buf) < 6 or txt_buf[:2] != '7E':
                continue
            pos = txt_buf[2:].find(FEND)
            if pos < 0:
                continue
            current_buf = txt_buf[:pos + 2]
            _LOGGER.debug(current_buf)
            txt_buf = txt_buf[pos + 4:]
            _LOGGER.debug(txt_buf)
            data_handler.add_data(current_buf)


if __name__ == '__main__':
    if DB_PATH:
        create_db()
    run()
