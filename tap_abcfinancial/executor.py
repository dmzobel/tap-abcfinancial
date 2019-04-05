import singer
import json
import pendulum
import datetime

from tap_kit import TapExecutor
from tap_kit.utils import timestamp_to_iso8601
from singer.catalog import Catalog, CatalogEntry
from tap_kit.utils import (transform_write_and_count,
                           format_last_updated_for_request)

LOGGER = singer.get_logger()


class ABCExecutor(TapExecutor):

    def __init__(self, streams, args, client):
        """
        Args:
            streams (arr[Stream])
            args (dict)
            client (BaseClient)
        """
        super(ABCExecutor, self).__init__(streams, args, client)

        self.replication_key_format = 'datetime_string'
        self.url = 'https://api.abcfinancial.com/rest/'
        self.api_key = self.client.config['api_key']
        self.app_id = self.client.config['app_id']

    def sync_stream(self, stream):
        stream.write_schema()
        LOGGER.info('stream:')
        LOGGER.info(stream)

        if stream.is_incremental:
            stream.set_stream_state(self.state)
            last_updated, club_id = self.call_incremental_stream(stream)
            stream.update_bookmark(last_updated, club_id)
        else:
            self.call_full_stream(stream)

    def call_incremental_stream(self, stream):
        """
        Method to call all incremental synced streams
        """

        # need to call each club ID individually
        for club_id in self.client.config['club_ids']:

            last_updated = format_last_updated_for_request(
                stream.update_and_return_bookmark(),
                self.replication_key_format
            )
            now_time = str(pendulum.now('UTC'))

            request_config = {
                'url': self.generate_api_url(stream, club_id),
                'headers': self.build_headers(),
                'params': self.build_initial_params(stream, last_updated, now_time),
                'run': True
            }

            LOGGER.info("Extracting {s} for club {c} since {d}".format(s=stream, 
                                                                       c=club_id, 
                                                                       d=last_updated))

            while request_config['run']:
                res = self.client.make_request(request_config)

                LOGGER.info('Received {n} records on page {i} for club {c}'.format(
                    n=res.json()['status']['count'],
                    i=res.json()['request']['page'],
                    c=club_id
                ))
                records = res.json().get(stream.stream, [])

                if stream.stream == 'prospects':
                    # the API does not provide club_id from the prospects endpoint
                    records = self.hydrate_record_with_club_id(records, club_id)

                transform_write_and_count(stream, records)

                request_config = self.update_for_next_call(
                    res,
                    request_config,
                    stream
                )

            LOGGER.info('Setting last updated for club {} to {}'.format(
                club_id,
                now_time
            ))

            return now_time, club_id

    def call_full_stream(self, stream):
        """
        Method to call all fully synced streams
        """
        for club_id in self.client.config['club_ids']:
            request_config = {
                'url': self.generate_api_url(stream, club_id),
                'headers': self.build_headers(),
                'params': self.build_params(stream),
                'run': True
            }

            LOGGER.info("Extracting {s} for club {c}".format(s=stream, 
                                                             c=club_id))

            while request_config['run']:
                res = self.client.make_request(request_config)

                LOGGER.info('Received {n} records on page {i} for club {c}'.format(
                    n=res.json()['status']['count'],
                    i=res.json()['request']['page'],
                    c=club_id
                ))

                records = res.json().get(stream.stream, [])

                transform_write_and_count(stream, records)

                request_config = self.update_for_next_call(res, request_config, stream)

    def generate_api_url(self, stream, club_id):
        return self.url + club_id + '/' + stream.stream

    def build_headers(self):
        """
        Included in all API calls
        """
        return {
            "Accept": "application/json;charset=UTF-8",  # necessary for returning JSON
            "app_id": self.app_id,
            "app_key": self.api_key,
        }

    def format_last_updated(self, last_updated):
        """
        Args:
            last_updated(str): datetime string in ISO 8601 format
        Return:
            datetime string in the following format: 'YYYY-MM-DD hh:mm:ss.nnnnnn'
            (necessary format for ABC Financial API)
        """
        datetime = pendulum.parse(last_updated).to_datetime_string() + '.000000'
        return datetime

    def build_initial_params(self, stream, last_updated, curr_time):
        date_range = '{l},{c}'.format(l=self.format_last_updated(last_updated),
                                      c=self.format_last_updated(curr_time))
        return {
            stream.stream_metadata[stream.filter_key]: date_range,
            'page': 1
        }

    def update_for_next_call(self, res, request_config, stream):
        if int(res.json()['status']['count']) == 0:  # must coerce value to a number
            return {
                "url": self.url,
                "headers": request_config['headers'],
                "params": request_config['params'],
                "run": False
            }
        else:
            return {
                "url": request_config['url'],
                "headers": request_config['headers'],
                "params": self.build_next_params(request_config['params']),
                "run": True
            }

    def build_next_params(self, params):
        params['page'] += 1
        return params

    def hydrate_record_with_club_id(self, records, club_id):
        """
        Args:
            records (array [JSON]):
        Returns:
            array of records, with the club_id appended to each record
        """
        for record in records:
            record['club_id'] = club_id

        return records
