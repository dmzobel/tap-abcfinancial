from tap_kit.streams import Stream
import singer


class MembersStream(Stream):

    stream = 'members'

    meta_fields = dict(
        key_properties=['memberId'],
        replication_method='incremental',
        replication_key='last_updated',
        incremental_search_key='lastModifiedTimestampRange',
        selected_by_default=False
    )

    schema = {
      "properties": {
      }
    }


class ProspectsStream(Stream):
    stream = 'prospects'

    meta_fields = dict(
        key_properties=['prospectId'],
        replication_method='incremental',
        replication_key='last_updated',
        incremental_search_key='lastCheckInTimestampRange',
        selected_by_default=False
    )

    schema = {
      "properties": {
      }
    }


class ClubsStream(Stream):
    stream = 'clubs'

    meta_fields = dict(
        key_properties=['id'],
        replication_method='full',
        selected_by_default=False
    )

    schema = {
      "properties": {
      }
    }
