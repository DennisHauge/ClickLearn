import sys
import requests
import json
import math
import re
import aiohttp
import asyncio

#######################################################################################################################
#
# Class definition
#
#######################################################################################################################


class Databox:

    """Databox toolkit for pushing data via API

    Attributes:
        <class> loop: Asyncio event loop
        <dict> payload: List of datapoints do be pushed
        <int> pushed: Number of successful pushes so far
        <int> pushes: Number of pushes pending parsing
        <class> queue: Asyncio queue containing PushAPI responses
        <dict> requestHeaders: HTTP Request Headers
        <int> requestSizeLimit: Maximum HTTP Request Size
        <str> requestURL: PushAPI Base URL
        <class> semaphore: Asyncio semaphore
        <in> semaphoreSize: Asyncio semaphore size
        <class> session: Requests Session
        <list> tokens: List of Data Source tokens to push data to
    """

    def __init__(self, _request_size_: int = 512000, _semaphore_size_: int = 5):

        """Initializes the instance

        Args:
            <int> _request_size_: Custom requestSizeLimit to use for HTTP requests to Databox
            <int> _semaphore_size_: Asyncio semaphore size
        """

        self.loop = None
        self.payload = {}
        self.pushed = 0
        self.pushes = 0
        self.queue = None
        self.requestHeaders = {
            'Accept': 'application/vnd.databox.v2+json',
            'Content-Type': 'application/json'
        }
        self.requestSizeLimit = _request_size_
        self.requestURL = 'https://push.databox.com'
        self.semaphore = None
        self.semaphoreSize = _semaphore_size_
        self.session = requests.Session()
        self.tokens = []

    def _api_exception(self, _loop_, _context_):

        """Handles exceptions raised during pushes

        Args:
            <class> _loop_: Asyncio event loop
            <exception> _context_: Exception raised

        Returns:
            <void>
        """

        self.loop.default_exception_handler(_context_)

        exception = _context_.get('exception')
        if isinstance(exception, Exception):
            print(_context_)
            self.loop.stop()

    async def _api_push(self, _token_: str, _datapoints_: list):

        """Sends HTTP POST Request to Databox

        Args:
            <str> _token_: Data Source token
            <list> _datapoints_: Datapoints to be pushed

        Returns:
            <void>
        """

        timeout = aiohttp.ClientTimeout(total=None, sock_connect=120, sock_read=120)
        client = aiohttp.ClientSession(headers=self.requestHeaders,
                                       auth=aiohttp.BasicAuth(_token_, ''), timeout=timeout)
        data = json.dumps({'data': _datapoints_})

        async with self.semaphore:
            async with client as session:
                async with session.post(url=self.requestURL, data=data) as response:
                    result = {
                        'token': _token_,
                        'data': json.loads(data),
                        "code": response.status,
                        'reason': response.reason,
                        'text': json.loads(await response.text())
                    }
                    await self.queue.put(result)
                    self.pushes -= 1
                    self.pushed += 1

    def _payload_process(self, _token_: str):

        """Processes the payload, i.e. breaks it down into manageable chunks that can be pushed to Databox

        Args:
            <str> _token_: Data Source token

        Returns:
            <list>: Processed list of list of datapoints, grouped under requestSizeLimit
        """

        datapoints = self.payload[_token_]

        size = sys.getsizeof(json.dumps(datapoints))
        chunks = math.ceil(size / self.requestSizeLimit)
        count = math.ceil(len(datapoints) / chunks)
        payload = []

        for chunk in self._payload_slice(datapoints, count):
            if not self._payload_validate(_token_, chunk):
                count_halved = math.ceil(len(chunk) / 2)
                for chunk_halved in self._payload_slice(chunk, count_halved):
                    payload.append(chunk_halved)
            else:
                payload.append(chunk)

        print(f'Databox > payload_process > Processed > {_token_} > {len(payload)} pushes')

        return payload

    async def _payload_push(self):

        """Pushes data to Databox using an event loop

        Args:
            <void>

        Returns:
            <void>
        """

        retries = 0
        self.queue = asyncio.Queue()
        self.semaphore = asyncio.Semaphore(self.semaphoreSize)

        for _token in self.tokens:
            chunks = self._payload_process(_token)
            for _chunk in chunks:
                self.loop.create_task(self._api_push(_token, _chunk))
                self.pushes += 1

        while self.pushes > 0:
            response = await self.queue.get()
            size = sys.getsizeof(json.dumps(response['data']['data']))
            if response['code'] == 200:
                print(f'Databox > payload_push > {self.pushed} > {response["token"]} > Success > {size} > '
                      f'{response["text"]["message"]}')
            elif response['code'] == 504 or response['code'] == 500:
                retries += 1
                if retries < 5:
                    print(response['data'])
                    self.loop.create_task(self._api_push(response['token'], response['data']['data']))
                    self.pushes += 1
                    print(f'Databox > payload_push > Retry > #{retries} > {response["token"]} > {size} > '
                          f'{response["code"]} {response["reason"]}')
                else:
                    raise Exception('Databox > PushAPI timed out 5 times, aborting ...')
            else:
                print(response['data'])
                raise Exception(f'Databox > payload_push > Exception > {str(response["code"])} {response["reason"]} > '
                                f'{response["text"]["message"]}')

    @staticmethod
    def _payload_slice(_list_: list, _n_: int):

        """Yield successive chunks from a list

        Args:
            <list> _list_: List of data points
            <int> _n_: Number of data points to return

        Yields:
            <list>: List of chunks or datapoints
        """

        for i in range(0, len(_list_), _n_):
            yield _list_[i:i + _n_]

    def _payload_validate(self, _token_: str, _datapoints_: list):

        """Check if the HTTP Request is under the maximum allowed as defined in the class attribute

        Args:
            <str> _token_: Data Source token
            <list> _datapoints_: Data to be pushed

        Returns:
            <bool>: Whether _datapoints_ can be pushed without going over the size limit
        """

        data = {'data': _datapoints_}

        request = requests.Request('POST',
                                   self.requestURL,
                                   data=json.dumps(data),
                                   headers=self.requestHeaders,
                                   auth=(_token_, ''))

        prepared = self.session.prepare_request(request)

        return False if int(prepared.headers['Content-Length']) > self.requestSizeLimit else True

    def append(self, _token_: str, _metrics_):

        """Appends a datapoint to the token's list of datapoints

        Args:
            <str> _token_: Data Source token
            <dict|list> _metric_: Datapoint dictionary or list (metric,[date,[dimension,[unit]]])

        Returns:
            <void>
        """

        if _token_ not in self.payload:
            self.payload[_token_] = []

        if isinstance(_metrics_, dict):
            self.payload[_token_].append(_metrics_)
        elif isinstance(_metrics_, list):
            self.payload[_token_] += _metrics_

    def push(self, _token_: str = None):

        """Triggers the push of all relevant data to Databox

        Args:
            <str> _token_: Data Source token to push to

        Returns:
            <void>
        """

        if not _token_:
            for _token in self.payload:
                self.tokens.append(_token)
        else:
            self.tokens.append(_token_)

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError as e:
            if str(e).startswith('There is no current event loop in thread'):
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
            else:
                raise
        self.loop.set_exception_handler(self._api_exception)
        self.loop.run_until_complete(self._payload_push())

    @staticmethod
    def sanitize(_key_: str, _metric_: bool = True):

        """Sanitizes the metric or dimension key to work with Databox

        Args:
            <str> _key_: Key text
            <bool> _metric_: Whether it is a metric or dimension key

        Returns:
            <str>: sanitized key text
        """

        _key = re.sub(r'[^a-zA-Z0-9_]', '_', str(_key_).title())
        return '$' + _key if _metric_ else _key

    @staticmethod
    def validate(_value_):

        """Sanitizes the metric or dimension key to work with Databox

        Args:
            <mixed> _value_: Metric value

        Returns:
            <mixed>: Validated metric value
        """

        if isinstance(_value_, int):

            # Integers cannot be bigger than 16 digits
            if len(str(_value_)) > 16:
                raise Exception(f'Databox > validate > Exception > Metric value too long > {_value_}')
            return _value_

        if isinstance(_value_, float) and not math.isnan(_value_):

            # Whole number portion cannot be bigger than 16 digits
            if len(str(_value_).split('.')[0]) > 16:
                raise Exception(f'Databox > validate > Exception > Metric value too long > {_value_}')

            # Floats can only have 6 digits precision
            return round(_value_, 6)

        else:
            # We don't know what that is
            raise Exception(f'Databox > validate > Exception > Invalid metric value > {_value_}')
