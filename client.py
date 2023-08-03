import json
import gzip
import requests

from point import escape_name


class OpenGeminiClient(object):
    def __init__(
            self,
            host='localhost',
            port=8086,
            username="",
            password="",
            database=None,
            session=None,
            gzip=False
    ):
        """
        Initialize client
        :param host: hostname to connect to OpenGeminiClient
        :param port: port to connect to OpenGeminiClient
        :param username: user to connect
        :param password: password of the user
        :param database: database name
        :param session: requests Session
        :param gzip: whether to use gzip
        """
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._baseurl = f"http://{self._host}:{self._port}"
        self._gzip = gzip
        if session is None:
            self._session = requests.Session()
        self._headers = {"Content-type": "application/json", "Accept": "text/plain"}
        self._write_headers = {"Content-type": "application/octet-stream", 
                               "Accept": "text/plain"}

    def request(
            self,
            url,
            method="GET",
            data=None,
            params=None,
            expected_response_code=200,
            header=None,
    ):
        """
        Make HTTP request
        :param url: the path of the HTTP request
        :param method: the HTTP method for the request
        :param data: the data of the request
        :param params: additional parameters for the request
        :param expected_response_code: the expected response code of the request
        :param header: request header
        :return: response
        """
        if header is None:
            header = self._headers
        if params is None:
            params = {}
        url = f"{self._baseurl}/{url}"
        if self._gzip:
            header.update({
                'Accept-Encoding': 'gzip',
                'Content-Encoding': 'gzip',
            })
            if data is not None:
                data = gzip.compress(data)

        response = self._session.request(
            method=method,
            url=url,
            auth=(self._username, self._password),
            data=data,
            headers=header,
            params=params,
        )
        if response.status_code == expected_response_code:
            return response
        else:
            raise Exception(f"{response.status_code}: {response.content}")

    def query(
            self,
            query,
            params=None,
            chunked=False,
            chunk_size=0,
            epoch=None,
            bind_params=None,
            method="GET"
    ):
        """
        Send a query to OpenGeminiClient.
        :param method: HTTP method
        :param query: query string
        :param params: additional parameters for the request
        :param chunked: Enable to use chunked responses
        :param chunk_size: Size of each chunk
        :param epoch: epoch format
        :param bind_params: bind parameters for the query
        :return: response
        """
        if params is None:
            params = {}
        if epoch is not None:
            params["epoch"] = epoch
        if chunked:
            params["chunked"] = "true"
            if chunk_size > 0:
                params["chunk_size"] = chunk_size
        if bind_params is not None:
            params_dict = json.loads(params.get("params", "{}"))
            params_dict.update(bind_params)
            params["params"] = json.dumps(params_dict)
        params["q"] = query
        if params.get("db", None) is None:
            params["db"] = self._database
        response = self.request(url="query", params=params, method=method)
        return response.json()

    def write(self, point, precision='ns'):
        params = {"db": self._database, "precision": precision}
        data = [point]
        data = ("\n".join(data) + "\n").encode("utf-8")
        self.request(
            url="write",
            method="POST",
            data=data,
            expected_response_code=204,
            header=self._write_headers,
            params=params,
        )

    def switch_database(self, database):
        self._database = database

    def switch_user(self, username, password):
        self._username = username
        self._password = password

    def ping(self):
        response = self.request(url="ping", method="GET", expected_response_code=204)
        return response.headers["X-Geminidb-Version"]

    def create_database(self, dbname):
        """
        Create a new database.
        """
        self.query(f"CREATE DATABASE {dbname}", method="POST")

    def drop_database(self, dbname):
        """
        Drop a database.
        """
        self.query(f"DROP DATABASE {dbname}", method="POST")

    def show_database(self):
        """
        show all database.
        """
        ret = self.query("SHOW DATABASES")
        return ret

    def create_retention_policy(
            self,
            retention_policy_name,
            replication,
            duration,
            database=None,
            default=False,
            shard_duration=None,
    ):
        """
        Create a retention policy for a database.
        :param retention_policy_name: the name of the new retention policy
        :param database: The database for which the retention policy is
            created. Defaults to current client's database
        :param duration: The DURATION clause determines how long openGemini will retain the data.
            The minimum duration of the retention policy is one hour and the maximum duration is INF (infinite).
        :param replication: The REPLICATION clause determines how many independent copies of
            each data point are stored in the cluster, currently only 1 copies are supported.
        :param default: Set the new retention policy as the default retention policy for the database.
            This setting is optional.
        :param shard_duration: The SHARD DURATION clause determines the time range of the slice group.
        """  # noqa: E501
        if database is None:
            database = self._database
        query = (
            f'CREATE RETENTION POLICY "{retention_policy_name.translate(escape_name)}" '
            f'ON "{database.translate(escape_name)}" '
            f'DURATION {duration} '
            f'REPLICATION {replication}'
        )
        if shard_duration:
            query += ' SHARD DURATION {shard_duration}'
        if default:
            query += ' DEFAULT'
        self.query(query, method="POST")

    def alter_retention_policy(
            self,
            retention_policy_name,
            database=None,
            duration=None,
            replication=None,
            default=None,
            shard_duration=None,
    ):
        """
        Modify retention policy for a database.
        :param retention_policy_name: the name of the retention policy to modify
        :param database: The database for which the retention policy is
            modified. Defaults to current client's database
        :param duration:The DURATION clause determines how long openGemini will retain the data.
            The minimum duration of the retention policy is one hour and the maximum duration is INF (infinite).
        :param replication:The REPLICATION clause determines how many independent copies of
            each data point are stored in the cluster, currently only 1 copies are supported.
        :param default: Set the retention policy as the default retention policy for the database.
            This setting is optional.
        :param shard_duration: The SHARD DURATION clause determines the time range of the slice group.

        note: Replication factor REPLICATION <n> Supported only 1
        """  # noqa: E501
        if database is None:
            database = self._database
        query = (
            f'ALTER RETENTION POLICY "{retention_policy_name.translate(escape_name)}" '
            f'ON "{database.translate(escape_name)}"'
        )
        if duration:
            query += f" DURATION {duration}"
        if shard_duration:
            query += f" SHARD DURATION {shard_duration}"
        if replication:
            query += f" REPLICATION {replication}"
        if default is True:
            query += " DEFAULT"

        self.query(query, method="POST")

    def drop_retention_policy(self, retention_policy_name, database=None):
        """
        Delete all measurement and data from the retention policy:
        Warning: Deleting a retention policy will permanently delete all measurements and data retained in the
        retention policy.
        :param retention_policy_name: the name of the retention policy to drop
        :param database: the database for which the retention policy is dropped. Defaults to current client's database
        """  # noqa: E501
        if database is None:
            database = self._database
        self.query(
            f'DROP RETENTION POLICY "{retention_policy_name.translate(escape_name)}" '
            f'ON "{database.translate(escape_name)}"', method="POST"
        )

    def show_retention_policy(self, database=None):
        """
        Returns a list of reservation policies for the specified database.
        :param database: the name of the database, defaults to the client's current database
        :return: Returns a list of reservation policies for the specified database.
        """  # noqa: E501
        if database is None:
            database = self._database
        ret = self.query(
            (f"SHOW RETENTION POLICIES ON "
             f'"{database.translate(escape_name)}"')
        )
        return ret

    def create_user(self, username, password, admin=False):
        """
        Create a new user.
        :param username: The desired username for the new user.
        :param password: The desired password for the new user.
        :param admin: Create an admin user when admin is True. Create a general user when admin is False.
        """  # noqa: E501
        query = f"CREATE USER {username} WITH PASSWORD {password}"
        if admin:
            query += " WITH ALL PRIVILEGES"
        self.query(query, method="POST")

    def drop_user(self, username):
        """Drop a user."""
        query = f"DROP USER {username}"
        self.query(query, method="POST")
