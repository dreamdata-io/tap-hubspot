import singer
from typing import DefaultDict, Set, Union, Dict
from datetime import timedelta, datetime
from dateutil import parser
from tap_hubspot.hubspot import Hubspot
import pytz

LOGGER = singer.get_logger()


class Stream:
    def __init__(self, config: Dict, tap_stream_id: str, stream_config: Dict):
        self.tap_stream_id = tap_stream_id
        self.bookmark_key = stream_config.get("bookmark_key")
        self.config = config

    def do_sync(self, state: Dict, hubspot: Hubspot):

        prev_bookmark = None
        start_date, end_date = self.__get_start_end(state)
        with singer.metrics.record_counter(self.tap_stream_id) as counter:

            try:
                data = hubspot.streams(
                    start_date=start_date,
                    end_date=end_date,
                    tap_stream_id=self.tap_stream_id,
                )
                for record, replication_value in data:

                    singer.write_record(self.tap_stream_id, record)
                    counter.increment(1)
                    if not replication_value:
                        continue

                    new_bookmark = replication_value
                    if not prev_bookmark:
                        prev_bookmark = new_bookmark

                    if prev_bookmark < new_bookmark:
                        state = self.__advance_bookmark(state, prev_bookmark)
                        prev_bookmark = new_bookmark
                return self.output_state(
                    state=state,
                    prev_bookmark=prev_bookmark,
                    event_state=hubspot.event_state,
                )

            finally:
                self.__advance_bookmark(state, prev_bookmark)

    def output_state(self, state, prev_bookmark, event_state):

        if self.tap_stream_id in [
            "contacts_events",
        ]:
            date_source = self.tap_stream_id.split("_")[0]
            prev_bookmark = event_state[f"{date_source}_end_date"]

        return (
            self.__advance_bookmark(state, prev_bookmark)
        )

    def __get_start_end(self, state: dict):
        end_date = pytz.utc.localize(datetime.utcnow())
        LOGGER.info(f"sync data until: {end_date}")

        config_start_date = self.config.get("start_date")
        if config_start_date:
            config_start_date = parser.isoparse(config_start_date)
        else:
            config_start_date = datetime.utcnow() + timedelta(weeks=4)

        if not state:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        account_record = state["bookmarks"].get(self.tap_stream_id, None)
        if not account_record:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        current_bookmark = account_record.get(self.bookmark_key, None)
        if not current_bookmark:
            LOGGER.info(f"using 'start_date' from config: {config_start_date}")
            return config_start_date, end_date

        start_date = parser.isoparse(current_bookmark)
        if self.tap_stream_id in ["contacts"]:
            # tracking data sync is dependent on contacts sync
            # hubspot does not return tracking data for contacts that are recently created
            # we need to always rewind 2 days to fetch the contacts
            start_date = start_date - timedelta(days=2)
        LOGGER.info(f"using 'start_date' from previous state: {start_date}")
        return start_date, end_date

    def __advance_bookmark(self, state: dict, bookmark: Union[str, datetime, None]):
        if not bookmark:
            singer.write_state(state)
            return state

        if isinstance(bookmark, datetime):
            bookmark_datetime = bookmark
        elif isinstance(bookmark, str):
            bookmark_datetime = parser.isoparse(bookmark)
        else:
            raise ValueError(
                f"bookmark is of type {type(bookmark)} but must be either string or datetime"
            )

        state = singer.write_bookmark(
            state, self.tap_stream_id, self.bookmark_key, bookmark_datetime.isoformat()
        )
        singer.write_state(state)
        return state
