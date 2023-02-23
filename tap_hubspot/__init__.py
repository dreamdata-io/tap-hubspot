#!/usr/bin/env python3
import shelve
import os
import tempfile
import singer
import sys
from singer import utils
from tap_hubspot.stream import Stream
from tap_hubspot.hubspot import InvalidCredentials, MissingScope
from collections import defaultdict
from typing import DefaultDict, Set

STREAMS = {
    "companies": {"bookmark_key": "updatedAt"},
    "owners": {"bookmark_key": "updatedAt"},
    "forms": {"bookmark_key": "updatedAt"},
    "contacts": {
        "bookmark_key": "updatedAt",
    },
    "contacts_events": {"bookmark_key": "lastSynced"},
    "contact_lists": {"bookmark_key": "lastSizeChangeAt"},
    "contacts_in_contact_lists": {},
    "deal_pipelines": {"bookmark_key": "updatedAt"},
    "submissions": {"bookmark_key": "submittedAt"},
    "email_events": {"bookmark_key": "created"},
    "deals": {"bookmark_key": "updatedAt"},
    "deal_properties": {"bookmark_key": "updatedAt"},
    "contact_properties": {"bookmark_key": "updatedAt"},
    "company_properties": {"bookmark_key": "updatedAt"},
    "archived_contacts": {"bookmark_key": "archivedAt"},
    "archived_companies": {"bookmark_key": "archivedAt"},
    "archived_deals": {"bookmark_key": "archivedAt"},
    "calls": {"bookmark_key": "lastUpdated"},
    "meetings": {"bookmark_key": "lastUpdated"},
    "notes": {"bookmark_key": "lastUpdated"},
    "tasks": {"bookmark_key": "lastUpdated"},
    "emails": {"bookmark_key": "lastUpdated"},
    "campaigns": {},
}
REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "redirect_uri",
]
LOGGER = singer.get_logger()


def sync(config, state=None):
    with tempfile.TemporaryDirectory(
        prefix=f"{os.getcwd()}/temp_event_state_"
    ) as temp_dirname:
        event_state: DefaultDict[Set, str] = defaultdict(set)

        event_state["contacts_events_ids"] = shelve.open(
            f"{temp_dirname}/contacts_events_ids"
        )
        event_state["hs_calculated_form_submissions_guids"] = shelve.open(
            f"{temp_dirname}/hs_calculated_form_submissions_guids"
        )

        for tap_stream_id, stream_config in STREAMS.items():
            try:
                LOGGER.info(f"syncing {tap_stream_id}")
                stream = Stream(
                    config=config,
                    tap_stream_id=tap_stream_id,
                    stream_config=stream_config,
                )
                state, event_state = stream.do_sync(state, event_state)
            except InvalidCredentials:
                LOGGER.exception(f"Invalid credentials")
                sys.exit(5)
            except MissingScope as err:
                LOGGER.exception(err)
                continue
            except Exception:
                LOGGER.exception(f"{tap_stream_id} failed")
                sys.exit(1)


@utils.handle_top_exception(LOGGER)
def main():

    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
