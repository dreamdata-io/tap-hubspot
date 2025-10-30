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
from typing import DefaultDict, Set, List
from tap_hubspot.models import Table

FREE_STREAMS = [
    Table(
        name="companies",
        bookmark_key="updatedAt",
        should_sync_properties=True,
    ),
    Table(
        name="contacts",
        bookmark_key="updatedAt",
        should_sync_properties=True,
    ),
    Table(
        name="deals",
        bookmark_key="updatedAt",
        should_sync_properties=True,
    ),
    Table(
        name="owners",
        bookmark_key="updatedAt",
    ),
    Table(
        name="archived_contacts",
        bookmark_key="archivedAt",
    ),
    Table(
        name="archived_companies",
        bookmark_key="archivedAt",
    ),
    Table(
        name="archived_deals",
        bookmark_key="archivedAt",
    ),
    Table(
        name="deal_pipelines",
        bookmark_key="updatedAt",
    ),
]
ADVANCED_STREAMS = [
    Table(
        name="forms",
        bookmark_key="updatedAt",
    ),
    Table(
        name="contacts_events",
        bookmark_key="lastSynced",
    ),
    Table(
        name="contact_lists",
        bookmark_key="lastSizeChangeAt",
    ),
    Table(
        name="contacts_in_contact_lists",
    ),
    Table(
        name="submissions",
        bookmark_key="submittedAt",
    ),
    Table(
        name="email_events",
        bookmark_key="created",
    ),
    Table(
        name="calls",
        bookmark_key="lastUpdated",
        should_sync_properties=True,
    ),
    Table(
        name="meetings",
        bookmark_key="lastUpdated",
        should_sync_properties=True,
    ),
    Table(
        name="notes",
        bookmark_key="lastUpdated",
        should_sync_properties=True,
    ),
    Table(
        name="tasks",
        bookmark_key="lastUpdated",
        should_sync_properties=True,
    ),
    Table(
        name="emails",
        bookmark_key="lastUpdated",
        should_sync_properties=True,
    ),
    Table(
        name="campaigns",
    ),
    Table(
        name="communications",
        bookmark_key="updatedAt",
        should_sync_properties=True,
    ),
    Table(
        name="marketing_events",
    ),
    Table(
        name="marketing_event_participations",
    ),
    Table(
        name="marketing_campaigns",
    ),
]

CUSTOM_STREAMS = [
    Table(
        name="p8915701_marketing_engagements",
        bookmark_key="updatedAt",
        should_sync_properties=True,
        is_custom_object=True,
        portal_id=8915701,
    ),
    # yodeck_com
    Table(
        name="p139792148_Yapp_Account",
        bookmark_key="updatedAt",
        should_sync_properties=True,
        is_custom_object=True,
        portal_id=139792148,
    ),
    # aldevron_com
    Table(
        name="tickets",
        bookmark_key="updatedAt",
        should_sync_properties=True,
        is_custom_object=True,
        portal_id=1769030,
    ),
]


REQUIRED_CONFIG_KEYS = [
    "start_date",
    "client_id",
    "client_secret",
    "refresh_token",
    "redirect_uri",
]

LOGGER = singer.get_logger()


def sync(config: dict, state=None):
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
        hubspot = Hubspot(config=config, event_state=event_state)
        tables = get_tables(
            advanced_features_enabled=config.get("advanced_features_enabled", False),
            portal_id=hubspot.get_portal_id(),
        )

        for table in tables:
            try:
                stream = Stream(
                    config=config,
                    tap_stream_id=table.name,
                    bookmark_key=table.bookmark_key,
                )
                if table.should_sync_properties:
                    LOGGER.info(f"syncing {table.name} properties")
                    stream.sync_properties(hubspot)
                LOGGER.info(f"syncing {table.name}")
                state = stream.do_sync(hubspot, table.is_custom_object, state)

            except InvalidCredentials:
                LOGGER.exception(f"Invalid credentials")
                sys.exit(5)
            except MissingScope as err:
                LOGGER.exception(err)
                continue
            except Exception:
                LOGGER.exception(f"{table.name} failed")
                sys.exit(1)


def get_tables(advanced_features_enabled: bool, portal_id: int) -> List[Table]:
    streams = FREE_STREAMS.copy()
    if advanced_features_enabled:
        LOGGER.info("advanced features enabled for account")
        streams = streams + ADVANCED_STREAMS
    for stream in CUSTOM_STREAMS:
        if not (portal_id == stream.portal_id):
            continue
        streams.append(stream)
    return streams


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    sync(args.config, args.state)


if __name__ == "__main__":
    main()
