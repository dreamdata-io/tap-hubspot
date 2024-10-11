from pydantic import BaseModel
from typing import List


class Filter(BaseModel):
    operator: str
    property: str
    propertyType: str
    values: List[str]


class EventSetting(BaseModel):
    object: str
    source_id: str
    filters: List[List[Filter]]


class EventSettings(BaseModel):
    event_settings: List[EventSetting]

    def get_unique_operators(self, object: str) -> set:
        operators = set()
        for event_setting in self.event_settings:
            if event_setting.object != object:
                continue
            for filter in event_setting.filters:
                for condition in filter:
                    operators.add(condition.operator)
        return operators

    def get_unique_values(self, object: str, property: str) -> set:
        values = set()
        for event_setting in self.event_settings:
            if event_setting.object != object:
                continue
            for filter in event_setting.filters:
                for condition in filter:
                    if condition.property != property:
                        continue
                    for value in condition.values:
                        values.add(value)
        return values
