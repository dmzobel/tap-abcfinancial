import singer
import pendulum

from tap_kit import TapExecutor
from tap_kit.utils import (transform_write_and_count,
                           format_last_updated_for_request)
from .streams import ABCStream

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

    def sync(self):
        self.set_catalog()

        for c in self.selected_catalog:
            self.sync_stream(
                ABCStream(config=self.config, state=self.state, catalog=c)
            )

    def sync_stream(self, stream):
        stream.write_schema()

        if stream.is_incremental:
            stream.set_stream_state(self.state)
            self.call_incremental_stream(stream)
        else:
            self.call_full_stream(stream)

    def call_incremental_stream(self, stream):
        """
        Method to call all incremental synced streams
        """

        # need to call each club ID individually
        for club_id in self.client.config['club_ids']:

            last_updated = format_last_updated_for_request(
                stream.update_and_return_bookmark(club_id),
                self.replication_key_format
            )
            new_bookmark = self.get_new_bookmark(stream, last_updated)

            request_config = {
                'url': self.generate_api_url(stream, club_id),
                'headers': self.build_headers(),
                'params': self.build_initial_params(stream, last_updated, new_bookmark),
                'run': True
            }

            LOGGER.info("Extracting {s} for club {c} from {d} to {n}".format(
                s=stream, c=club_id, d=last_updated, n=new_bookmark)
            )

            final_bookmark = self.call_stream(stream, club_id, request_config,
                                              new_bookmark)

            LOGGER.info('Setting {s} last updated for club {c} to {b}'.format(
                s=stream,
                c=club_id,
                b=final_bookmark
            ))

            stream.update_bookmark(final_bookmark, club_id)

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

            self.call_stream(stream, club_id, request_config)

    def call_stream(self, stream, club_id, request_config, last_updated=None):
        while request_config['run']:
            res = self.client.make_request(request_config)

            if stream.is_incremental:
                LOGGER.info('Received {n} records on page {i} for club {c}'.format(
                    n=res.json()['status']['count'],
                    i=res.json()['request']['page'],
                    c=club_id
                ))
            else:
                LOGGER.info('Received {n} records for club {c}'.format(
                    n=res.json()['status']['count'],
                    c=club_id
                ))

            records = res.json().get(stream.stream_metadata['response-key'])

            if records:
                if not isinstance(records, list):
                    # subsequent methods are expecting a list
                    records = [records]

                # for endpoints that do not provide club_id
                if stream.stream in streams_to_hydrate:
                    records = self.hydrate_record_with_club_id(records, club_id)

            transform_write_and_count(stream, records)

            # TODO: this is hacky and should be rethought
            # since the checkins bookmark could be changing along the way (see
            # self.get_new_bookmark), this is how we keep track of it
            if stream.stream == 'checkins':
                request_dates = res.json()['request'][stream.stream_metadata[stream.filter_key]]
                last_updated = request_dates.split(',')[1]

            LOGGER.info('{s} bookmark for club {c} is set at: {b}'.format(
                s=stream.stream, c=club_id, b=last_updated)
            )

            request_config = self.update_for_next_call(
                int(res.json()['status']['count']),
                request_config,
                stream,
                last_updated
            )

        return last_updated

    def generate_api_url(self, stream, club_id):
        return self.url + club_id + stream.stream_metadata['api-path']

    def build_headers(self):
        """
        Included in all API calls
        """
        return {
            "Accept": "application/json;charset=UTF-8",  # necessary for returning JSON
            "app_id": self.app_id,
            "app_key": self.api_key,
        }

    def get_new_bookmark(self, stream, last_updated):
        # the checkins endpoint only extracts in 31 day windows, so
        # `new_bookmark` needs to account for that
        if stream.stream == 'checkins':
            dt = pendulum.parse(last_updated) \
                 if last_updated != '1970-01-01 00:00:00' \
                 else pendulum.datetime(2015, 1, 1)  # min lookback date is January 1, 2015

            new_bookmark = min(dt.add(days=31), pendulum.now('UTC'))
        else:
            new_bookmark = str(pendulum.now('UTC'))

        return str(new_bookmark)

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

    def build_initial_params(self, stream, last_updated, new_bookmark):
        date_range = '{p},{c}'.format(p=self.format_last_updated(last_updated),
                                      c=self.format_last_updated(new_bookmark))
        return {
            stream.stream_metadata[stream.filter_key]: date_range,
            'page': 1
        }

    def update_for_next_call(self, num_records_received, request_config,
                             stream, last_updated=None):
        if num_records_received < 5000:
            # since the checkins stream only extracts in 31 day increments,
            # we don't want it to stop until it's reached the present day.
            # therefore, we need to handle it separately
            if stream.stream == 'checkins' and \
                    pendulum.parse(last_updated) < pendulum.today('UTC'):
                return self.get_next_config_for_checkins(stream, last_updated,
                                                         request_config)
            else:
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
        if params.get('page'):
            params['page'] += 1
        return params

    def get_next_config_for_checkins(self, stream, last_updated,
                                     request_config):

        new_bookmark = self.get_new_bookmark(stream, last_updated)

        return {
            "url": request_config['url'],
            "headers": request_config['headers'],
            "params": self.build_initial_params(stream, last_updated, new_bookmark),
            "run": True
        }

    def hydrate_record_with_club_id(self, records, club_id):
        """
        Args:
            records (array [JSON]):
            club_id (str):
        Returns:
            array of records, with the club_id appended to each record
        """
        for record in records:
            record['club_id'] = club_id

        return records


streams_to_hydrate = ['prospects', 'clubs', 'checkins']
