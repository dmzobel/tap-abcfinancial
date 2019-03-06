from tap_kit import TapExecutor
from tap_kit.utils import timestamp_to_iso8601
from singer.catalog import Catalog, CatalogEntry
from tap_kit.utils import (transform_write_and_count, format_last_updated_for_request)

import json
import time
import pendulum
import singer
import sys
import datetime
import pytz

LOGGER = singer.get_logger()


class PerkvilleExecutor(TapExecutor):
    """
    """

    def __init__(self, streams, args, client):
        """
        """
        super(PerkvilleExecutor, self).__init__(streams, args, client)

        # login_request_config = {
        #     'url': LOGIN,
        #     'params': {
        #         'grant_type': 'refresh_token',
        #         'client_id': self.client.config['client_id'],
        #         'client_secret': self.client.config['client_secret'],
        #         'refresh_token': self.client.config['refresh_token'],
        #     },
        #     'headers': {}
        # }

        # response_dict = self.client.make_request(
        #     login_request_config
        # ).json()
        self.replication_key_format = 'timestamp'
        self.access_token = self.client.config['access_token']
        self.url = 'https://api.perkville.com/v2/'

        # self.config['execution_start_time_ts'] = pendulum.from_timestamp(
        #     time.time(),
        #     tz='local'
        # )

    def call_incremental_stream(self, stream):
        """
        Method to call all incremental synced streams
        """
        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(), self.replication_key_format)

        request_config = {
            'url': self.generate_api_url(stream),
            'headers': self.build_headers(),
            'params': self.build_initial_params(stream, last_updated),
            'run': True
        }

        LOGGER.info("Extracting %s since %s" % (stream, last_updated))


        while request_config['run']:

            LOGGER.info("Params: %s" % (request_config['params']))
            res = self.client.make_request(request_config)

            if res.status_code != 200:
                raise AttributeError('Received status code {}'.format(res.status_code))

            records = res.json()['objects']

            LOGGER.info('Received {} records'.format(len(records)))

            if self.should_write(records, stream, last_updated):
                transform_write_and_count(stream, records)
            
            last_updated = self.get_lastest_update(
                records,
                last_updated
            )

            stream.update_bookmark(last_updated)

            request_config = self.update_for_next_call(
                res,
                request_config,
                stream
            )


        return last_updated

    def get_lastest_update(self, records, last_updated):
        max_updated = last_updated
        for rec in records:
            date =  datetime.datetime.strptime(rec['last_mod_dt'][0:19], '%Y-%m-%d %H:%M:%S')
            max_updated = max(max_updated, date.timestamp())
        return int(max_updated)

    def build_initial_params(self, stream, last_updated=None):
        return {
            'last_mod_dt__gte': self.format_last_modified(last_updated),
            'limit': 1000,
            'offset': 0
        }

    def format_last_modified(self, last_updated):

        date = datetime.datetime.fromtimestamp(last_updated, tz=pytz.UTC)
        last_mod = '{} {}:{}Z'.format(
            date.strftime('%Y-%m-%d'),
            date.strftime('%H'),
            date.strftime('%M')
        )
        return last_mod
    
    def update_for_next_call(self, res, request_config, stream):
        
        if len(res.json()['objects']) == 0:
            return {
                "url": self.url,
                "headers": request_config["headers"],
                "params": request_config['params'],
                "run": False
            }

        return {
            "url": self.generate_api_url(stream),
            "headers": request_config["headers"],
            "params": self.build_next_params(request_config['params']),
            "run": True
        }

    def build_next_params(self, params):
        params['offset'] += params['limit']
        return params

    def build_headers(self):
        """Included in all API calls, aside from logging in.
        """
        return {
            "Authorization": "Bearer {}".format(self.access_token),
        }
