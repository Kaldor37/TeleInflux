from datetime import datetime
from typing import Optional, Dict, Any, Union

from serial import Serial


class TeleinfoFrame:
    """
    Represents a Teleinfo framed, read with TeleinfoReader
    """

    _FIELD_FORMAT = {
        'ADCO': 'str',
        'OPTARIF': 'str',
        'ISOUSC': 'int',
        'HCHC': 'int',
        'HCHP': 'int',
        'PTEC': 'str',
        'IINST': 'int',
        'IMAX': 'int',
        'PAPP': 'int',
        'HHPHC': 'str',
        'MOTDETAT': 'str',
    }

    def __init__(self, raw_data: Dict[str, bytes]):
        """
        Constructor
        :param raw_data: raw data loaded from Teleinfo stream
        """
        self._raw_data = raw_data
        self._utc_time = datetime.utcnow()

    def __str__(self):
        return str(self.format_fields())

    @staticmethod
    def _format_str(value: bytes) -> str:
        return value.decode().rstrip('.')

    @staticmethod
    def _format_int(value: bytes) -> int:
        return int(value.decode())

    def get(self, key: str) -> Union[str, int]:
        """
        Access a field of this Teleinfo frame
        :param key: field name
        :return: formatted field
        """
        field_format = self._FIELD_FORMAT.get(key, 'str')
        field_formatter = f'_format_{field_format}'
        assert field_formatter
        return getattr(self, field_formatter)(self._raw_data.get(key, ''))

    def format_fields(self) -> Dict[str, Union[str, int]]:
        return {key: self.get(key) for key in self._raw_data.keys()}

    def set(self, key: str, value: Any):
        self._raw_data[key] = str(value).encode()

    @property
    def utc_time(self) -> datetime:
        return self._utc_time


class TeleinfoException(Exception):
    """
    Represents any TeleinfoException raised during reading of Teleinfo stream
    """
    pass


class TeleinfoReader:
    """
    Reads teleinfo from a file
    """

    _SERIAL_BAUD_RATE = 1200
    _SERIAL_BYTESIZE = 7
    _SERIAL_TIMEOUT = 30

    def __init__(self, input_file: str = None, is_serial_port: bool = True):
        if is_serial_port:
            self._reader = Serial(
                port=input_file,
                baudrate=self._SERIAL_BAUD_RATE,
                bytesize=self._SERIAL_BYTESIZE,
                timeout=self._SERIAL_TIMEOUT
            )
        else:
            self._reader = open(input_file, 'rb')

        self._read_buffer = bytearray()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self._reader.close()

    def read_frame(self) -> Optional[TeleinfoFrame]:
        """
        Reads Teleinfo stream and returns the next full frame
        """
        assert self._reader

        frame_buffer = bytearray()
        started = False
        while True:
            chunk_data = self._reader.read(1)
            if not chunk_data:
                return None

            for b in chunk_data:
                if started:
                    # End of frame
                    if b == 3:
                        return self._parse_frame(bytes(frame_buffer))
                    else:
                        frame_buffer.append(b)

                # Start of frame data
                elif b == 2:
                    started = True

    @staticmethod
    def _parse_frame(frame_data: bytes) -> TeleinfoFrame:
        """
        Parses a Teleinfo frame from raw data
        :param frame_data: raw frame data
        :return: the parse TeleinfoFrame
        """
        parsed_frame = {}
        for line in frame_data.strip().split(b'\r\n'):
            label, value, checksum = line.split(b' ', 2)

            _sum = (sum(int(b) for b in line[0:-2]) & 0x3F) + 0x20
            if _sum != checksum[0]:
                raise TeleinfoException(
                    f'Checksum control failed for {line}, {_sum} != {checksum[0]} - Frame data: {frame_data}'
                )

            parsed_frame[label.decode()] = value

        return TeleinfoFrame(parsed_frame)
