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
            "memberId": {
                "type": ["null", "string"]
            },
            "personal": {
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
                    "type": ["null", "boolean"]
                },
                "memberStatus": {
                    "type": ["null", "string"]
                },
                "joinStatus": {
                    "type": ["null", "string"]
                },
                "isConvertedProspect": {
                    "type": ["null", "boolean"]
                },
                "hasPhoto": {
                    "type": ["null", "boolean"]
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
                },
                "type": ["null", "struct"]
            },
            "agreement": {
                "agreementNumber": {
                    "type": ["null", "string"]
                },
                "isPrimaryMember": {
                    "type": ["null", "boolean"]
                },
                "isNonMember": {
                    "type": ["null", "boolean"]
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
                    "type": ["null", "boolean"]
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
                    "firstName": {
                        "type": ["null", "string"]
                    },
                    "lastName": {
                        "type": ["null", "string"]
                    },
                    "type": ["null", "struct"]
                },
                "type": ["null", "struct"]
            }
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
            "prospectId": {
                "type": ["null", "string"]
            },
            "personal": {
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
                    "type": ["null", "boolean"]
                },
                "hasPhoto": {
                    "type": ["null", "boolean"]
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
                },
                "type": ["null", "struct"]
            },
            "agreement": {
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
                },
                "type": ["null", "struct"]
            },
            "club_id": {
                "type": ["null", "string"]
            }
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
                "minors": {
                    "allowMinors": {
                        "type": ["null", "boolean"]
                    },
                    "minorAge": {
                        "type": ["null", "string"]
                    },
                    "minorDisclaimer": {
                        "type": ["null", "string"]
                    },
                    "type": ["null", "struct"]
                },
                "ccNames": {
                    "requireCCNameMatch": {
                        "type": ["null", "boolean"]
                    },
                    "differentCcNamesDisclaimer": {
                        "type": ["null", "string"]
                    },
                    "type": ["null", "struct"]
                },
                "showFees": {
                    "type": ["null", "boolean"]
                },
                "type": ["null", "struct"]
            },
            "billingCountry": {
                "type": ["null", "string"]
            },
            "creditCardPaymentMethods": {
                "type": ["null", "array"]
            },
            "thirdPartyPaymentMethods": {
                "type": ["null", "array"]
            }
        }
    }
