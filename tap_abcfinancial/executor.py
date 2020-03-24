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

            final_bookmark = self.call_stream(stream, club_id, request_config, new_bookmark)

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

    def call_stream(self, stream, club_id, request_config, curr_upper_bound=None):
        """
        Utility method shared by incremental and full streams; handles API calls and
        record writes
        """
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

            if not records:
                records = []
            elif not isinstance(records, list):
                # subsequent methods are expecting a list
                records = [records]

            # for endpoints that do not provide club_id
            if stream.stream in STREAMS_TO_HYDRATE:
                records = self.hydrate_record_with_club_id(records, club_id)

            transform_write_and_count(stream, records)

            if stream.is_incremental:
                LOGGER.info('{s} bookmark for club {c} is currently {b}'.format(
                    s=stream.stream, c=club_id, b=curr_upper_bound)
                )

            request_config, curr_upper_bound = self.update_for_next_call(
                int(res.json()['status']['count']),
                request_config,
                stream,
                curr_upper_bound
            )

        return curr_upper_bound

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

    @staticmethod
    def get_new_bookmark(stream, last_updated):
        # some streams (checkins, events) only extract in 30 day windows, so
        # `new_bookmark` needs to account for that
        if stream.stream in ('checkins', 'events'):
            last_updated = pendulum.parse(last_updated)
            # for checkins, API does not appear to return any records < 7 hours old
            # add 12 hour delay, so we're not requesting records that are not yet available
            upper_bound = pendulum.now('UTC').subtract(hours=12)
            new_bookmark = min(last_updated.add(days=30), upper_bound)
        else:
            new_bookmark = str(pendulum.now('UTC'))

        return str(new_bookmark)

    @staticmethod
    def format_last_updated(last_updated):
        """
        Args:
            last_updated(str): datetime string in ISO 8601 format
        Returns:
            datetime string in the following format: 'YYYY-MM-DD hh:mm:ss.nnnnnn'
            (necessary format for ABC Financial API)
        """
        datetime = pendulum.parse(last_updated).to_datetime_string() + '.000000'
        return datetime

    def build_initial_params(self, stream, last_updated, new_bookmark):
        date_range = '{p},{c}'.format(p=self.format_last_updated(last_updated),
                                      c=self.format_last_updated(new_bookmark))
        return {
            stream.stream_metadata['incremental-search-key']: date_range,
            'page': 1
        }

    def update_for_next_call(self, num_records_received, request_config,
                             stream, last_updated=None):
        """
        We return `last_updated` so that it can be easily referenced in other functions
        without having to string-parse the time range provided in the request config
        Returns:
            tuple (request_config (dict), last_updated datetime (str))
        """
        if num_records_received < 5000:
            # some streams (checkins, events) only extract in 30 day increments,
            # we don't want them to stop until they've reached the present day.
            # therefore, we need to handle them differently than "normal" streams
            cutoff_dt = pendulum.now('UTC').subtract(hours=12).start_of('day')
            if stream.stream in ('checkins', 'events') and \
                    pendulum.parse(last_updated) < cutoff_dt:
                return self.get_next_config_for_30day_streams(stream,
                                                              last_updated,
                                                              request_config)
            else:
                request_config['run'] = False
                return request_config, last_updated
        else:
            new_config = {"url": request_config['url'],
                          "headers": request_config['headers'],
                          "params": self.build_next_params(request_config['params']),
                          "run": True}
            return new_config, last_updated

    @staticmethod
    def build_next_params(params):
        if params.get('page'):
            params['page'] += 1
        return params

    def get_next_config_for_30day_streams(self, stream, last_updated, request_config):
        """
        Returns:
             tuple (request_config (dict), last_updated datetime (str))
        """
        new_bookmark = self.get_new_bookmark(stream, last_updated)
        new_config = {
            "url": request_config['url'],
            "headers": request_config['headers'],
            "params": self.build_initial_params(stream, last_updated, new_bookmark),
            "run": True
        }
        return new_config, new_bookmark

    @staticmethod
    def hydrate_record_with_club_id(records, club_id):
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


STREAMS_TO_HYDRATE = {'prospects', 'clubs', 'checkins', 'events'}
