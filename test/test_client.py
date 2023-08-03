import unittest
from client import OpenGeminiClient
from point import Point
import requests_mock
import gzip


class TestOpenGeminiClient(unittest.TestCase):
    def setUp(self):
        """Initialize an instance of TestOpenGeminiClient object."""
        self.cli = OpenGeminiClient('localhost', 8086, 'username', 'password')
        self.point = (Point('weather')
                      .tag('location', 'us-midwest')
                      .field('temperature', 82)
                      .time(1465839830100400200))

    def test_scheme(self):
        """Set up the test schema for TestOpenGeminiClient object."""
        cli = OpenGeminiClient('localhost', 8086, 'username', 'password')
        self.assertEqual('http://localhost:8086', cli._baseurl)

    def test_switch_database(self):
        """Test switch database in TestOpenGeminiClient object."""
        cli = OpenGeminiClient('localhost', 8086, 'username', 'password', 'database')
        cli.switch_database('another_database')
        self.assertEqual('another_database', cli._database)

    def test_switch_user(self):
        """Test switch user in TestOpenGeminiClient object."""
        cli = OpenGeminiClient('host', 8086, 'username', 'password', 'database')
        cli.switch_user('another_username', 'another_password')
        self.assertEqual('another_username', cli._username)
        self.assertEqual('another_password', cli._password)

    def test_write(self):
        """Test write in TestOpenGeminiClient object."""
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/write",
                status_code=204
            )
            self.cli.write(self.point.to_line_protocol())
            self.assertEqual(
                m.last_request.body,
                b"weather,location=us-midwest "
                b"temperature=82i 1465839830100400200\n",
            )

    def test_write_gzip(self):
        """Test write in TestOpenGeminiClient object."""
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.POST,
                "http://localhost:8086/write",
                status_code=204
            )
            cli = OpenGeminiClient(database='db', gzip=True)
            line = self.point.to_line_protocol()
            cli.write(line)
            compressed_data = gzip.compress((line+'\n').encode('utf-8'))
            self.assertEqual(
                m.last_request.headers.get('Content-Encoding', ''),
                'gzip'
            )
            self.assertEqual(
                m.last_request.body,
                compressed_data
            )

    def test_query(self):
        """Test query method for TestOpenGeminiClient object."""
        example_response = {"results": [{"statement_id": 0, "series": [
                {"name": "weather", "columns": ["time", "location", "temperature"],
                 "values": [["2016-06-13T17:43:50.1004002Z", "us-midwest", 82]]}]}]}
        with requests_mock.Mocker() as m:
            m.register_uri(
                requests_mock.GET,
                "http://localhost:8086/query",
                json=example_response
            )
            self.cli.switch_database("database")
            rs = self.cli.query('select * from weather')
            self.assertEqual(
                rs,
                example_response
            )