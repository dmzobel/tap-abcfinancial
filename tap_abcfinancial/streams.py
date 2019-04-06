from tap_kit.streams import Stream
from tap_kit.utils import safe_to_iso8601
import singer

LOGGER = singer.get_logger()


class ABCStream(Stream):
    """
    methods to track state for each individual ABC Financial club
    """

    def __init__(self, config=None, state=None, catalog=None):
        super(ABCStream, self).__init__(config, state, catalog)

        self.config = config
        self.state = state
        self.catalog = catalog
        self.api_path = self.api_path if self.api_path else self.stream

        self.build_params()

    def write_bookmark(self, state, tap_stream_id, club_id, key, val):
        state = self.ensure_bookmark_path(state, ['bookmarks',
                                                  tap_stream_id,
                                                  club_id])
        state['bookmarks'][tap_stream_id][club_id][key] = val
        return state

    @staticmethod
    def ensure_bookmark_path(state, path):
        """
        :param state: state object
        :param path: array of keys to check in state
        :return: checks for or creates a nested object in which each element
        of the path array is the parent key of the next element
        """
        submap = state
        for path_component in path:
            if submap.get(path_component) is None:
                submap[path_component] = {}

            submap = submap[path_component]
        return state

    def get_bookmark(self, club_id):
        key = self.stream_metadata.get('replication-key')

        return self.state.get('bookmarks', {})\
                         .get(self.stream, {})\
                         .get(club_id, {})\
                         .get(key)

    def update_bookmark(self, last_updated, club_id):
        self.write_bookmark(self.state,
                            self.stream,
                            club_id,
                            self.stream_metadata.get('replication-key'),
                            safe_to_iso8601(last_updated))
        singer.write_state(self.state)

    def update_start_date_bookmark(self, club_id):
        val = self.get_bookmark(club_id)
        if not val:
            val = self.config['start_date']
            self.update_bookmark(val, club_id)

    def update_and_return_bookmark(self, club_id):
        self.update_start_date_bookmark(club_id)
        return self.get_bookmark(club_id)

    @property
    def is_incremental(self):
        if self.stream_metadata.get('forced-replication-method') == 'incremental':
            return True
        else:
            return False

    def write_schema(self):
        singer.write_schema(
            self.catalog.stream,
            self.catalog.schema.to_dict(),
            key_properties=self.stream_metadata.get('table-key-properties', []))


class MembersStream(ABCStream):

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
            "memberId": {
                "type": ["null", "string"]
            },
            "personal": {
                "properties": {
                    "firstName": {
                        "type": ["null", "string"]
                    },
                    "lastName": {
                        "type": ["null", "string"]
                    },
                    "middleInitial": {
                        "type": ["null", "string"]
                    },
                    "addressLine1": {
                        "type": ["null", "string"]
                    },
                    "city": {
                        "type": ["null", "string"]
                    },
                    "state": {
                        "type": ["null", "string"]
                    },
                    "postalCode": {
                        "type": ["null", "string"]
                    },
                    "homeClub": {
                        "type": ["null", "string"]
                    },
                    "countryCode": {
                        "type": ["null", "string"]
                    },
                    "email": {
                        "type": ["null", "string"]
                    },
                    "primaryPhone": {
                        "type": ["null", "string"]
                    },
                    "workPhoneExt": {
                        "type": ["null", "string"]
                    },
                    "emergencyExt": {
                        "type": ["null", "string"]
                    },
                    "barcode": {
                        "type": ["null", "string"]
                    },
                    "birthDate": {
                        "type": ["null", "string"]
                    },
                    "gender": {
                        "type": ["null", "string"]
                    },
                    "isActive": {
                        "type": ["null", "string"]
                    },
                    "memberStatus": {
                        "type": ["null", "string"]
                    },
                    "joinStatus": {
                        "type": ["null", "string"]
                    },
                    "isConvertedProspect": {
                        "type": ["null", "string"]
                    },
                    "hasPhoto": {
                        "type": ["null", "string"]
                    },
                    "memberStatusReason": {
                        "type": ["null", "string"]
                    },
                    "firstCheckInTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    "lastCheckInTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    "totalCheckInCount": {
                        "type": ["null", "string"]
                    },
                    "createTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    "lastModifiedTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    }
                },
                "type": ["null", "object"]
            },
            "agreement": {
                "properties": {
                    "agreementNumber": {
                        "type": ["null", "string"]
                    },
                    "isPrimaryMember": {
                        "type": ["null", "string"]
                    },
                    "isNonMember": {
                        "type": ["null", "string"]
                    },
                    "ordinal": {
                        "type": ["null", "string"]
                    },
                    "salesPersonId": {
                        "type": ["null", "string"]
                    },
                    "salesPersonName": {
                        "type": ["null", "string"]
                    },
                    "salesPersonHomeClub": {
                        "type": ["null", "string"]
                    },
                    "paymentPlan": {
                        "type": ["null", "string"]
                    },
                    "paymentPlanId": {
                        "type": ["null", "string"]
                    },
                    "term": {
                        "type": ["null", "string"]
                    },
                    "paymentFrequency": {
                        "type": ["null", "string"]
                    },
                    "membershipType": {
                        "type": ["null", "string"]
                    },
                    "managedType": {
                        "type": ["null", "string"]
                    },
                    "campaignId": {
                        "type": ["null", "string"]
                    },
                    "campaignName": {
                        "type": ["null", "string"]
                    },
                    "isPastDue": {
                        "type": ["null", "string"]
                    },
                    "renewalType": {
                        "type": ["null", "string"]
                    },
                    "agreementPaymentMethod": {
                        "type": ["null", "string"]
                    },
                    "downPayment": {
                        "type": ["null", "string"]
                    },
                    "nextDueAmount": {
                        "type": ["null", "string"]
                    },
                    "pastDueBalance": {
                        "type": ["null", "string"]
                    },
                    "lateFeeAmount": {
                        "type": ["null", "string"]
                    },
                    "serviceFeeAmount": {
                        "type": ["null", "string"]
                    },
                    "totalPastDueBalance": {
                        "type": ["null", "string"]
                    },
                    "clubAccountPastDueBalance": {
                        "type": ["null", "string"]
                    },
                    "currentQueue": {
                        "type": ["null", "string"]
                    },
                    "queueTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    "agreementEntrySource": {
                        "type": ["null", "string"]
                    },
                    "agreementEntrySourceReportName": {
                        "type": ["null", "string"]
                    },
                    "sinceDate": {
                        "type": ["null", "string"]
                    },
                    "beginDate": {
                        "type": ["null", "string"]
                    },
                    "firstPaymentDate": {
                        "type": ["null", "string"]
                    },
                    "signDate": {
                        "type": ["null", "string"]
                    },
                    "nextBillingDate": {
                        "type": ["null", "string"]
                    },
                    "primaryBillingAccountHolder": {
                        "properties": {
                            "firstName": {
                                "type": ["null", "string"]
                            },
                            "lastName": {
                                "type": ["null", "string"]
                            }
                        },
                        "type": ["null", "object"]
                    }
                },
                "type": ["null", "object"]
            }
        }
    }


class ProspectsStream(ABCStream):
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
            "prospectId": {
                "type": ["null", "string"]
            },
            "personal": {
                "properties": {
                    "firstName": {
                        "type": ["null", "string"]
                    },
                    "lastName": {
                        "type": ["null", "string"]
                    },
                    "city": {
                        "type": ["null", "string"]
                    },
                    "state": {
                        "type": ["null", "string"]
                    },
                    "postalCode": {
                        "type": ["null", "string"]
                    },
                    "countryCode": {
                        "type": ["null", "string"]
                    },
                    "primaryPhone": {
                        "type": ["null", "string"]
                    },
                    "barcode": {
                        "type": ["null", "string"]
                    },
                    "birthDate": {
                        "type": ["null", "string"]
                    },
                    "gender": {
                        "type": ["null", "string"]
                    },
                    "isActive": {
                        "type": ["null", "string"]
                    },
                    "hasPhoto": {
                        "type": ["null", "string"]
                    },
                    "firstCheckInTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    "createdTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    },
                    "lastModifiedTimestamp": {
                        "type": ["null", "string"],
                        "format": "date-time"
                    }
                },
                "type": ["null", "object"]
            },
            "agreement": {
                "properties": {
                    "referringMemberId": {
                        "type": ["null", "string"]
                    },
                    "referringMemberHomeClub": {
                        "type": ["null", "string"]
                    },
                    "referringMemberName": {
                        "type": ["null", "string"]
                    },
                    "salesPersonId": {
                        "type": ["null", "string"]
                    },
                    "salesPersonName": {
                        "type": ["null", "string"]
                    },
                    "salesPersonHomeClub": {
                        "type": ["null", "string"]
                    },
                    "campaignId": {
                        "type": ["null", "string"]
                    },
                    "campaignName": {
                        "type": ["null", "string"]
                    },
                    "campaignGroup": {
                        "type": ["null", "string"]
                    },
                    "agreementEntrySource": {
                        "type": ["null", "string"]
                    },
                    "agreementEntrySourceReportName": {
                        "type": ["null", "string"]
                    },
                    "beginDate": {
                        "type": ["null", "string"]
                    },
                    "expirationDate": {
                        "type": ["null", "string"]
                    },
                    "issueDate": {
                        "type": ["null", "string"]
                    },
                    "tourDate": {
                        "type": ["null", "string"]
                    },
                    "visitsAllowed": {
                        "type": ["null", "string"]
                    },
                    "visitsUsed": {
                        "type": ["null", "string"]
                    }
                },
                "type": ["null", "object"]
            },
            "club_id": {
                "type": ["null", "string"]
            }
        }
    }


class ClubsStream(ABCStream):
    stream = 'clubs'

    meta_fields = dict(
        key_properties=['id'],
        replication_method='full',
        selected_by_default=False
    )

    schema = {
        "properties": {
            "name": {
                "type": ["null", "string"]
            },
            "shortName": {
                "type": ["null", "string"]
            },
            "timeZone": {
                "type": ["null", "string"]
            },
            "address1": {
                "type": ["null", "string"]
            },
            "city": {
                "type": ["null", "string"]
            },
            "state": {
                "type": ["null", "string"]
            },
            "postalCode": {
                "type": ["null", "string"]
            },
            "country": {
                "type": ["null", "string"]
            },
            "email": {
                "type": ["null", "string"]
            },
            "onlineSignupAllowedPaymentMethods": {
                "type": ["null", "string"]
            },
            "supportedCountries": {
                "type": ["null", "array"]
            },
            "online": {
                "properties": {
                    "minors": {
                        "properties": {
                            "allowMinors": {
                                "type": ["null", "string"]
                            },
                            "minorAge": {
                                "type": ["null", "string"]
                            },
                            "minorDisclaimer": {
                                "type": ["null", "string"]
                            }
                        },
                        "type": ["null", "object"]
                    },
                    "ccNames": {
                        "properties": {
                            "requireCCNameMatch": {
                                "type": ["null", "string"]
                            },
                            "differentCcNamesDisclaimer": {
                                "type": ["null", "string"]
                            }
                        },
                        "type": ["null", "object"]
                    },
                    "showFees": {
                        "type": ["null", "string"]
                    }
                },
                "type": ["null", "object"]
            },
            "billingCountry": {
                "type": ["null", "string"]
            },
            "creditCardPaymentMethods": {
                "type": ["null", "array"]
            },
            "thirdPartyPaymentMethods": {
                "type": ["null", "array"]
            },
            "club_id": {
                "type": ["null", "string"]
            }
        }
    }
