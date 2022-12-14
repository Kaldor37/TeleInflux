import configparser
import logging
import os.path
import signal
import time
from typing import Dict, Any

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError

from teleinflux.teleinfo import TeleinfoReader, TeleinfoException


class Teleinflux:
    """
    Reads Teleinfo data and writes in an InfluxDB measurement
    """

    def __init__(self):
        self._config = configparser.ConfigParser()

        if os.path.isfile('/etc/teleinflux.conf'):
            self._config.read('/etc/teleinflux.conf')

        elif os.path.isfile('/etc/teleinflux/teleinflux.conf'):
            self._config.read('/etc/teleinflux/teleinflux.conf')

        elif os.path.isfile('teleinflux.conf'):
            self._config.read('teleinflux.conf')

        logging.basicConfig(
            filename=self._config.get('logging', 'file', fallback='teleinflux.log'),
            level=getattr(logging, self._config.get('logging', 'level', fallback='INFO').upper()),
            format=self._config.get('logging', 'format', fallback='%(asctime)s [%(levelname)s] %(message)s')
        )

        self._influx_client = InfluxDBClient(
            host=self._config.get('database', 'host', fallback='localhost'),
            port=self._config.getint('database', 'port', fallback=8086),
            username=self._config.get('database', 'username', fallback='teleinflux'),
            password=self._config.get('database', 'password', fallback='teleinflux')
        )
        self._influx_database = self._config.get('database', 'database_name', fallback='teleinflux')
        self._influx_write_attemps = self._config.getint('database', 'write_attempts', fallback=3)

        signal.signal(signal.SIGINT, self._sig_handler)
        signal.signal(signal.SIGTERM, self._sig_handler)

        self._input_file = self._config.get('input', 'file', fallback=None)
        self._serial_port = self._config.get('input', 'serial_port', fallback=None)

        self._running = True

    def _sig_handler(self, signo, _sigframe):
        logging.info(f'Received stop signal: {signo}')
        self._running = False

    def run(self):
        logging.info('Running')

        if not self._influx_client.ping():
            logging.error('Failed to ping InfluxDB')
            exit(1)

        if self._influx_database not in self._influx_client.get_list_database():
            self._influx_client.create_database(self._influx_database)

        self._influx_client.switch_database(self._influx_database)

        with TeleinfoReader(**{
            'input_file': self._serial_port or self._input_file,
            'is_serial_port': bool(self._serial_port)
        }) as reader:
            while self._running:
                try:
                    # In case we're loading data from a file, we simulate the serial port latency
                    if not self._serial_port:
                        time.sleep(2)

                    frame = reader.read_frame()
                except TeleinfoException as ex:
                    logging.warning(f'Failed to read frame: {ex}')
                    continue

                if not frame:
                    logging.warning('Nothing to read on input stream')
                    break

                logging.debug(f'Read frame: {frame}')
                self._write_measurement('teleinfo', frame.utc_time.isoformat(), frame.format_fields())

        logging.info('Done')

    def _write_measurement(
        self, measurement_name: str, utc_time: str, fields: Dict[str, Any], tags: Dict[str, Any] = None
    ):
        """
        Writes any measurement to DB
        :param measurement_name: name of the measurement
        :param utc_time: time (UTC) of the measurement to write as iso formatted string
        :param fields: fields in the measurement
        :return: True on success
        """
        for __ in range(self._influx_write_attemps):
            try:
                measurement_data = {
                    'measurement': measurement_name,
                    'time': utc_time,
                    'fields': fields
                }
                if tags:
                    measurement_data['tags'] = tags

                if self._influx_client.write_points([measurement_data]):
                    return True

            except InfluxDBServerError as ex:
                logging.warning(f'Failed to write measurement {measurement_name}: {ex} - {fields}')

            time.sleep(0.1)

        logging.error(
            f'Failed to write measurement {measurement_name} within {self._influx_write_attemps} attempts: {fields}'
        )
        return False
