from tap_kit.streams import Stream
import singer


class ConnectionsStream(Stream):

    stream = 'connections'

    meta_fields = dict(
        key_properties=['id'],
        replication_method='incremental',
        replication_key='last_updated',
        incremental_search_key='updated_after',
        selected_by_default=False
    )

    schema = \
    {
      "properties": {
      }
    }

