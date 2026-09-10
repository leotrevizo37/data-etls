import base64
import json
import os
import re
from datetime import date, time
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dagster import resource


RESPONSE_FIELDS = {
    "SUCCESS",
    "MESSAGE",
    "ERROR_CODE",
    "HTTP_STATUS",
    "COUNT",
    "HAS_MORE",
    "NEXT_CURSOR",
    "STORES",
}
STORE_FIELDS = {
    "STORE_ID",
    "CUSTOMER_NO",
    "NAME",
    "OPENING_DATE",
    "CLOSING_DATE",
    "BLOCKED",
    "BLOCKED_FROM",
    "BLOCKED_TO",
    "RECEIVING_ID",
    "SCHEDULE",
}
SCHEDULE_FIELDS = {"DAY", "DAY_NO", "CLOSED", "SLOTS"}
SLOT_FIELDS = {"FROM", "TO"}
DAY_CODES = {
    1: "MON",
    2: "TUE",
    3: "WED",
    4: "THU",
    5: "FRI",
    6: "SAT",
    7: "SUN",
}
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
TIME_PATTERN = re.compile(r"\d{2}:\d{2}:\d{2}")


class StoreSchedulesApiResource:
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        timeout_seconds: int,
        page_size: int,
    ) -> None:
        self.url = url
        self.user = user
        self.password = password
        self.timeout_seconds = timeout_seconds
        self.page_size = page_size

    def fetch_page(self, cursor: Optional[str] = None) -> Dict[str, Any]:
        if (
            not self.url
            or not self.user
            or not self.password
            or self.timeout_seconds <= 0
            or not 1 <= self.page_size <= 500
        ):
            raise RuntimeError("Store schedules API configuration is incomplete.")
        body = {
            "prefix": "9999",
            "includeClosed": True,
            "pageSize": self.page_size,
        }
        if cursor is not None:
            body["cursor"] = cursor
        authorization = base64.b64encode(
            f"{self.user}:{self.password}".encode("utf-8")
        ).decode("ascii")
        request = Request(
            self.url,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Authorization": "Basic " + authorization,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                if response.status != 200:
                    raise RuntimeError(
                        f"Store schedules API returned HTTP {response.status}."
                    )
                if response.headers.get_content_type() != "application/json":
                    raise RuntimeError("Store schedules API did not return JSON.")
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise RuntimeError(
                f"Store schedules API returned HTTP {exc.code}."
            ) from exc
        except URLError as exc:
            raise RuntimeError("Store schedules API request failed.") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("Store schedules API returned invalid JSON.") from exc
        return payload

    def validate_page(self, payload: Any) -> Dict[str, Any]:
        if type(payload) is not dict or set(payload) != {"result"}:
            raise ValueError("Store schedules API response does not match the contract.")
        result = payload["result"]
        if type(result) is not dict or set(result) != {"RESPONSE_HORARIO"}:
            raise ValueError("Store schedules API result does not match the contract.")
        response = result["RESPONSE_HORARIO"]
        if type(response) is not dict or set(response) != RESPONSE_FIELDS:
            raise ValueError("Store schedules API fields do not match the contract.")
        if type(response["SUCCESS"]) is not bool:
            raise ValueError("Store schedules API field SUCCESS must be boolean.")
        if response["SUCCESS"] is not True:
            error_code = response["ERROR_CODE"]
            raise ValueError(
                "Store schedules API returned SUCCESS=false"
                + (f" ({error_code})." if isinstance(error_code, str) and error_code else ".")
            )
        for field in ("MESSAGE", "ERROR_CODE", "NEXT_CURSOR"):
            if type(response[field]) is not str:
                raise ValueError(
                    f"Store schedules API field {field} must be a string."
                )
        for field in ("HTTP_STATUS", "COUNT"):
            if type(response[field]) is not int:
                raise ValueError(
                    f"Store schedules API field {field} must be an integer."
                )
        if type(response["HAS_MORE"]) is not bool:
            raise ValueError("Store schedules API field HAS_MORE must be boolean.")
        if type(response["STORES"]) is not list:
            raise ValueError("Store schedules API field STORES must be an array.")
        if response["HTTP_STATUS"] != 200:
            raise ValueError("Store schedules API HTTP_STATUS must be 200 on success.")
        if response["COUNT"] != len(response["STORES"]):
            raise ValueError("Store schedules API COUNT does not match STORES.")
        if response["COUNT"] > self.page_size:
            raise ValueError("Store schedules API COUNT exceeds the requested page size.")
        if response["HAS_MORE"] and not response["NEXT_CURSOR"]:
            raise ValueError("Store schedules API pagination cursor is missing.")
        if not response["HAS_MORE"] and response["NEXT_CURSOR"]:
            raise ValueError("Store schedules API returned an unexpected next cursor.")

        store_ids = set()
        for store_index, store in enumerate(response["STORES"]):
            self._validate_store(store, store_index)
            store_id = store["STORE_ID"].casefold()
            if store_id in store_ids:
                raise ValueError("Store schedules API page contains duplicate stores.")
            store_ids.add(store_id)
        return response

    def _validate_store(self, store: Any, store_index: int) -> None:
        if type(store) is not dict or set(store) != STORE_FIELDS:
            raise ValueError(
                f"Store schedules API store {store_index} fields do not match the contract."
            )
        string_limits = {
            "STORE_ID": 4,
            "CUSTOMER_NO": 20,
            "NAME": 250,
            "OPENING_DATE": 10,
            "CLOSING_DATE": 10,
            "BLOCKED_FROM": 10,
            "BLOCKED_TO": 10,
            "RECEIVING_ID": 20,
        }
        for field, limit in string_limits.items():
            value = store[field]
            if type(value) is not str or len(value) > limit:
                raise ValueError(
                    f"Store schedules API store {store_index} field {field} is invalid."
                )
        if not store["STORE_ID"]:
            raise ValueError(
                f"Store schedules API store {store_index} STORE_ID is empty."
            )
        if type(store["BLOCKED"]) is not bool:
            raise ValueError(
                f"Store schedules API store {store_index} BLOCKED must be boolean."
            )
        if type(store["SCHEDULE"]) is not list:
            raise ValueError(
                f"Store schedules API store {store_index} SCHEDULE must be an array."
            )
        parsed_dates = {}
        for field in ("OPENING_DATE", "CLOSING_DATE", "BLOCKED_FROM", "BLOCKED_TO"):
            value = store[field]
            if not value:
                parsed_dates[field] = None
                continue
            if not DATE_PATTERN.fullmatch(value):
                raise ValueError(
                    f"Store schedules API store {store_index} field {field} is invalid."
                )
            try:
                parsed_dates[field] = date.fromisoformat(value)
            except ValueError as exc:
                raise ValueError(
                    f"Store schedules API store {store_index} field {field} is invalid."
                ) from exc
        if (
            parsed_dates["OPENING_DATE"] is not None
            and parsed_dates["CLOSING_DATE"] is not None
            and parsed_dates["OPENING_DATE"] > parsed_dates["CLOSING_DATE"]
        ):
            raise ValueError(
                f"Store schedules API store {store_index} operating dates are invalid."
            )

        day_numbers = set()
        for schedule in store["SCHEDULE"]:
            if type(schedule) is not dict or set(schedule) != SCHEDULE_FIELDS:
                raise ValueError(
                    "Store schedules API schedule fields do not match the contract."
                )
            if type(schedule["DAY"]) is not str or type(schedule["DAY_NO"]) is not int:
                raise ValueError("Store schedules API schedule day is invalid.")
            if DAY_CODES.get(schedule["DAY_NO"]) != schedule["DAY"]:
                raise ValueError("Store schedules API schedule day is inconsistent.")
            if schedule["DAY_NO"] in day_numbers:
                raise ValueError("Store schedules API store contains duplicate days.")
            day_numbers.add(schedule["DAY_NO"])
            if type(schedule["CLOSED"]) is not bool or type(schedule["SLOTS"]) is not list:
                raise ValueError("Store schedules API schedule state is invalid.")
            slots = schedule["SLOTS"]
            if schedule["CLOSED"] and slots:
                raise ValueError("Store schedules API closed day contains slots.")
            if not schedule["CLOSED"] and not 1 <= len(slots) <= 2:
                raise ValueError("Store schedules API open day must contain one or two slots.")
            parsed_slots = []
            for slot in slots:
                if type(slot) is not dict or set(slot) != SLOT_FIELDS:
                    raise ValueError(
                        "Store schedules API slot fields do not match the contract."
                    )
                parsed_slot = []
                for field in ("FROM", "TO"):
                    value = slot[field]
                    if type(value) is not str or not TIME_PATTERN.fullmatch(value):
                        raise ValueError("Store schedules API slot time is invalid.")
                    try:
                        parsed_slot.append(time.fromisoformat(value))
                    except ValueError as exc:
                        raise ValueError("Store schedules API slot time is invalid.") from exc
                if parsed_slot[0] >= parsed_slot[1]:
                    raise ValueError("Store schedules API slot range is invalid.")
                parsed_slots.append(parsed_slot)
            if len(parsed_slots) == 2 and parsed_slots[0][1] > parsed_slots[1][0]:
                raise ValueError("Store schedules API slots overlap.")
        if day_numbers != set(DAY_CODES):
            raise ValueError(
                f"Store schedules API store {store_index} does not contain all seven days."
            )

    @staticmethod
    def normalize_stores(stores: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        rows = []
        for store in stores:
            for schedule in store["SCHEDULE"]:
                slots = schedule["SLOTS"]
                rows.append(
                    {
                        "StoreId": store["STORE_ID"],
                        "CustomerNo": store["CUSTOMER_NO"],
                        "StoreName": store["NAME"],
                        "OpeningDate": store["OPENING_DATE"] or None,
                        "ClosingDate": store["CLOSING_DATE"] or None,
                        "IsBlocked": store["BLOCKED"],
                        "BlockedFrom": store["BLOCKED_FROM"] or None,
                        "BlockedTo": store["BLOCKED_TO"] or None,
                        "ReceivingId": store["RECEIVING_ID"] or None,
                        "DayCode": schedule["DAY"],
                        "DayNo": schedule["DAY_NO"],
                        "IsClosed": schedule["CLOSED"],
                        "OpeningTime": slots[0]["FROM"] if slots else None,
                        "LunchStartTime": slots[0]["TO"] if len(slots) == 2 else None,
                        "LunchEndTime": slots[1]["FROM"] if len(slots) == 2 else None,
                        "ClosingTime": slots[-1]["TO"] if slots else None,
                    }
                )
        return rows


@resource
def store_schedules_api_resource(_):
    return StoreSchedulesApiResource(
        url=os.getenv("BAFAR_STORE_SCHEDULES_API_URL", ""),
        user=os.getenv("BAFAR_STORE_SCHEDULES_API_USER", ""),
        password=os.getenv("BAFAR_STORE_SCHEDULES_API_PASSWORD", ""),
        timeout_seconds=int(
            os.getenv("BAFAR_STORE_SCHEDULES_API_TIMEOUT_SECONDS", "30")
        ),
        page_size=int(os.getenv("BAFAR_STORE_SCHEDULES_API_PAGE_SIZE", "100")),
    )
