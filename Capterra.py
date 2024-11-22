import datetime
import json
import time
import pytz
from dateutil.relativedelta import relativedelta
import databox
import asyncio
import aiohttp
import pandas

TIME_BEGIN = datetime.datetime.now()
pandas.set_option('display.max_rows', None)
pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', 2000)


#######################################################################################################################
#
# Class definition
#
#######################################################################################################################

class Capterra:
    """ Capterra class

    Attributes:
        <class> loop: Asyncio event loop
        <int> requests: Total number of API request made
        <dict> requestHeaders: HTTP Request Headers
        <class> requestTimeout: Asyncio ClientTimeout instance
        <str> requestURL: API Base URL
        <class> semaphore: Asyncio semaphore
        <str> token: Data Source token
    """

    def __init__(self, _semaphore_size_=5):

        """Initializes the instance

        Args:
            <int> _semaphore_size_: Asyncio semaphore size
        """

        self.accessToken = '9oNpR3ALYMoyJTXk'
        self.DataboxToken = '83d1bf67ff0c42b8a70a4b9bd979599c'
        self.requests = 0
        self.requestHeaders = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': self.accessToken
        }
        self.requestTimeout = aiohttp.ClientTimeout(total=None, sock_connect=240, sock_read=240)
        self.requestURL = 'https://public-api.capterra.com/v1'
        self.semaphore = asyncio.Semaphore(_semaphore_size_)

        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception)

    def exception(self, _loop_, _context_):

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

    async def fetch(self, _endpoint_, _params_):

        """Makes an API request to Capterra API

        Args:
            <str> _token_: Data Source token
            <str> _endpoint_: API endpoint
            <dict> _params_: API request params
            <dict> _body_: API request body

        Returns:
            <dict>: Dictionary with API response data
        """

        client = aiohttp.ClientSession(headers=self.requestHeaders, timeout=self.requestTimeout)

        async with self.semaphore:
            async with client as session:
                url = self.requestURL + _endpoint_

                print('Capterra >', url, '>', json.dumps(_params_))

                async with session.get(url=url, params=_params_) as response:
                    result = {
                        'endpoint': _endpoint_,
                        'params': _params_,
                        'code': response.status,
                        'reason': response.reason,
                        'text': json.loads(await response.text())
                    }
                    self.requests += 1
                    return result

    def create_count_metric(self, pandasdataframe, date_column, value_column, metric_name, dimension=None):
        """From a Pandas DataFrame create a dictionary in the format required by the Databox API for a Metric
        Args:
            <pandas.DataFrame> pandasdataframe: DataFrame to create the metric from
            <str> date_column: Name of the DataFrame column to be used as date
            <str> value_column: Name of the DataFrame column to be used as value
            <str> metric_name: the metric name to be sent to databox
            <str> dimension: the name of the DataFrame column to be used as dimension
        """
        if dimension is not None:
            new_metric = pandasdataframe.groupby([date_column, dimension])[value_column].nunique() \
                .reset_index().to_dict(orient="index")
        else:
            new_metric = pandasdataframe.groupby([date_column])[value_column].nunique() \
                .reset_index().to_dict(orient="index")

        for index, row in new_metric.items():
            data = {
                '$' + metric_name: row[value_column],
                'date': row[date_column],
            }
            if dimension is not None:
                data[dimension.title()] = row[dimension]

            databox.append(self.DataboxToken, data)

    def create_sum_metric(self, pandasdataframe, date_column, value_column, metric_name, dimension=None):
        """From a Pandas DataFrame create a dictionary in the format required by the Databox API for a Metric
        Args:
            <pandas.DataFrame> pandasdataframe: DataFrame to create the metric from
            <str> date_column: Name of the DataFrame column to be used as date
            <str> value_column: Name of the DataFrame column to be used as value
            <str> metric_name: the metric name to be sent to databox
            <str> dimension: the name of the DataFrame column to be used as dimension
        """

        if dimension is None:
            new_metric = pandasdataframe.groupby([date_column])[value_column].sum() \
                .reset_index().to_dict(orient="index")
        else:
            new_metric = pandasdataframe.groupby([date_column, dimension])[value_column].sum() \
                .reset_index().to_dict(orient="index")

        for index, row in new_metric.items():
            data = {
                '$' + metric_name.title(): row[value_column],
                'date': row[date_column]
            }
            if dimension is not None:
                data[dimension] = row[dimension]

            databox.append(self.DataboxToken, data)

    async def clicks(self):

        """Fetches all relevant clicks data
        Args:
            <int> _monthOffset_: Sync period (Last X months)

        Returns:
            <list>: Dictionary with datapoints
        """
        # time transformations
        tz = pytz.timezone('UTC')
        dt_today = datetime.datetime.fromtimestamp(time.time(), tz=tz)
        dt_start = (dt_today.replace(day=1) - relativedelta(months=2))

        endpoint = '/clicks'
        data = []
        params = {
            'start_date': dt_start.strftime("%Y-%m-%d")
            # 'scroll_id': None
        }

        while True:
            result = await self.fetch(endpoint, params)
            clicks_data = result['text']['data']

            for click in clicks_data:
                data.append({
                    'date_created': click['date_of_report'],
                    'category': click['category'],
                    'Product': click['product_name'],
                    'clicks': click['clicks'],
                    'cost': click['cost'],
                    'avg_position': click['avg_position'],
                    'country':  click['country'],
                    'conversions': click['conversions'],
                    'cpl': click['cpl'],
                    'channel': click['channel'],
                })

            if 'scroll_id' in result['text']:
                params['scroll_id'] = result['text']['scroll_id']
            else:
                break

        return data


#######################################################################################################################
#
# Define Metrics
#
#######################################################################################################################


async def main():
    capterra = Capterra()
    metrics = ['clicks', 'conversions', 'cpl', 'cost', 'avg_position']
    dimensions = [None, 'category', 'channel', 'country']

    clicks_df = pandas.DataFrame(await capterra.clicks())

    for dimension_name in dimensions:
        for metric_name in metrics:
            capterra.create_sum_metric(pandasdataframe=clicks_df, date_column='date_created',
                                       value_column=metric_name, metric_name=metric_name, dimension=dimension_name)

    print('Total API Requests:', capterra.requests)


#######################################################################################################################
#
# Execute
#
#######################################################################################################################

databox = databox.Databox()

asyncio.run(main())

databox.push()

print(datetime.datetime.now() - TIME_BEGIN)

