import singer
import backoff

from tap_kit import BaseClient, RateLimitException

LOGGER = singer.get_logger()


class ABCClient(BaseClient):
    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def make_request(self, request_config, body=None, method='GET'):
        LOGGER.info("Making {} request to {}".format(
            method, request_config['url']))

        with singer.metrics.Timer('request_duration', {}) as timer:
            response = self.requests_method(method, request_config, body)

        if response.status_code in [401, 429, 503]:
            raise RateLimitException()

        response.raise_for_status()

        return response
