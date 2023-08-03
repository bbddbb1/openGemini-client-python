import math
from datetime import datetime

from dateutil.parser import parse
from pytz import UTC

EPOCH = UTC.localize(datetime.utcfromtimestamp(0))

escape_measurement = str.maketrans({
    ',': r'\,',
    ' ': r'\ ',
    '\n': r'\n',
    '\t': r'\t',
    '\r': r'\r',

})
escape_field_value = str.maketrans({
    '"': r'\"',
    '\\': r'\\',
})
escape_name = str.maketrans({
    ',': r'\,',
    ' ': r'\ ',
    '=': r'\=',
    '\n': r'\n',
    '\t': r'\t',
    '\r': r'\r',
})


def _to_nanos(timestamp):
    delta = timestamp - EPOCH
    nanos_in_days = delta.days * 86400 * 10 ** 9
    nanos_in_seconds = delta.seconds * 10 ** 9
    nanos_in_micros = delta.microseconds * 10 ** 3
    return nanos_in_days + nanos_in_seconds + nanos_in_micros


def _convert_timestamp(timestamp):
    """
    Converts a timestamp to nanoseconds since Unix epoch.
    :param timestamp: The timestamp to convert. Can be an integer, string, or datetime object.
    :return: The timestamp in nanoseconds.
    :raises ValueError: If the timestamp cannot be converted.
    """  # noqa: E501
    if timestamp is None:
        return ""

    if isinstance(timestamp, int):
        return timestamp  # assume precision is correct if timestamp is int

    if isinstance(timestamp, str):
        timestamp = parse(timestamp)

    if isinstance(timestamp, datetime):
        if not timestamp.tzinfo:
            timestamp = UTC.localize(timestamp)
        return _to_nanos(timestamp)

    raise ValueError(timestamp)


class Point(object):
    def __init__(self, measurement):
        self._tags = {}
        self._fields = {}
        self._measurement = measurement
        self._time = None
        self._write_precision = 'ns'
        self._field_types = {}

    def get_precision(self):
        return self._write_precision

    def time(self, time, write_precision='ns'):
        """
        set time with specified precision
        """
        self._write_precision = write_precision
        self._time = time
        return self

    def tag(self, key, value):
        """
        Add tag
        """
        if isinstance(key, str) and isinstance(value, str):
            self._tags[key] = value
        if isinstance(key, (list, tuple)) and isinstance(value, (list, tuple)):
            assert len(key) == len(value)
            for k, v in zip(key, value):
                self._tags[k] = v
        return self

    def field(self, field, value):
        """
        Add field
        """
        if isinstance(field, str) and isinstance(value, (str, float, bool, int)):
            self._fields[field] = value
        if isinstance(field, (list, tuple)) and isinstance(value, (list, tuple)):
            assert len(field) == len(value)
            for k, v in zip(field, value):
                self._fields[k] = v
        return self

    def to_line_protocol(self):
        measurement = self._measurement.translate(escape_measurement)
        tags = ''
        for key, value in sorted(self._tags.items()):
            if value is None:
                continue
            tag = key.translate(escape_name)
            value = value.translate(escape_name)
            if tag != '' and value != '':
                tags += f',{tag}={value}'

        fields = ''
        for key, value in self._fields.items():
            if value is None:
                continue
            field = key.translate(escape_name)
            if isinstance(value, str):
                fields += f',{field}="{value.translate(escape_field_value)}"'
            elif isinstance(value, bool):
                fields += f',{field}={str(value).lower()}'
            elif isinstance(value, float):
                if not math.isfinite(value):
                    continue
                fields += f',{field}={str(value)}'
            elif isinstance(value, int):
                fields += f',{field}={str(value)}i'
        if fields == '':
            return ''
        # trim the start ','
        fields = fields[1:]

        time = _convert_timestamp(self._time)
        time_format = f' {time}' if time != '' else ''
        return f'{measurement}{tags} {fields}{time_format}'
