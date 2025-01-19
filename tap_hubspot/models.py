from pydantic import BaseModel
from typing import List, Optional


class Filter(BaseModel):
    operator: Optional[str]
    property: Optional[str]
    propertyType: Optional[str]
    values: Optional[List[str]]


class EventSetting(BaseModel):
    object: str
    source_id: Optional[str]
    filters: Optional[List[List[Filter]]]


class Table(BaseModel):
    name: str
    bookmark_key: Optional[str]
    should_sync_properties: Optional[bool] = False
    is_custom_object: Optional[bool] = False
    portal_id: Optional[int]


class EventSettings(BaseModel):
    event_settings: List[EventSetting]

    def get_unique_operators(self, object: str) -> set:
        operators = set()
        for event_setting in self.event_settings:
            if event_setting.object != object:
                continue
            if not event_setting.filters:
                continue
            for filter in event_setting.filters:
                for condition in filter:
                    if not condition.operator:
                        continue
                    operators.add(condition.operator)
        return operators

    def get_unique_values(self, object: str, property: str) -> set:
        values = set()
        for event_setting in self.event_settings:
            if event_setting.object != object:
                continue
            if not event_setting.filters:
                continue
            for filter in event_setting.filters:
                for condition in filter:
                    if condition.property != property:
                        continue
                    for value in condition.values:
                        values.add(value)
        return values
