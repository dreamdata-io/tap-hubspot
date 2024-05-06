#!/usr/bin/env python3
import shelve
import os
import tempfile
import singer
import sys
from singer import utils
from tap_hubspot.stream import Stream
from tap_hubspot.hubspot import Hubspot, InvalidCredentials, MissingScope
from collections import defaultdict
from typing import DefaultDict, Set

FREE_STREAMS = {
    "companies": {"bookmark_key": "updatedAt"},
    "contacts": {"bookmark_key": "updatedAt"},
    "deals": {"bookmark_key": "updatedAt"},
    "owners": {"bookmark_key": "updatedAt"},
    "deal_properties": {"bookmark_key": "updatedAt"},
    "contact_properties": {"bookmark_key": "updatedAt"},
    "company_properties": {"bookmark_key": "updatedAt"},
    "archived_contacts": {"bookmark_key": "archivedAt"},
    "archived_companies": {"bookmark_key": "archivedAt"},
    "archived_deals": {"bookmark_key": "archivedAt"},
    "deal_pipelines": {"bookmark_key": "updatedAt"},
}

ADVANCED_STREAMS = {
    "forms": {"bookmark_key": "updatedAt"},
    "contacts_events": {"bookmark_key": "lastSynced"},
    "contact_lists": {"bookmark_key": "lastSizeChangeAt"},
    "contacts_in_contact_lists": {},
    "submissions": {"bookmark_key": "submittedAt"},
    "email_events": {"bookmark_key": "created"},
    "note_properties": {"bookmark_key": "updatedAt"},
    "task_properties": {"bookmark_key": "updatedAt"},
    "call_properties": {"bookmark_key": "updatedAt"},
    "meeting_properties": {"bookmark_key": "updatedAt"},
    "email_properties": {"bookmark_key": "updatedAt"},
    "calls": {"bookmark_key": "lastUpdated"},
    "meetings": {"bookmark_key": "lastUpdated"},
    "notes": {"bookmark_key": "lastUpdated"},
    "tasks": {"bookmark_key": "lastUpdated"},
    "emails": {"bookmark_key": "lastUpdated"},
    "campaigns": {},
    "communications": {"bookmark_key": "updatedAt"},
    "communication_properties": {"bookmark_key": "updatedAt"}
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

        streams = FREE_STREAMS.copy()
        advanced_features_enabled = config.pop("advanced_features_enabled", False)
        if advanced_features_enabled:
            LOGGER.info("advanced features enabled for account")
            streams.update(ADVANCED_STREAMS)
        hubspot = Hubspot(
                    config = config, 
                    event_state = event_state
                )
        for tap_stream_id, stream_config in streams.items():
            try:
                LOGGER.info(f"syncing {tap_stream_id}")
                stream = Stream(
                    config=config,
                    tap_stream_id=tap_stream_id,
                    stream_config=stream_config,
                )

                state = stream.do_sync(state, hubspot)
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
