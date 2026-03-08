"""
ManageEngine SDP On-Demand → Kafka Producer
Region: UAE  →  https://sdpondemand.manageengine.me

Pulls ALL requests (tickets) via paginated API and publishes each one as a
JSON message to the Kafka topic:  sdp.tickets.raw

Run:
    python producers/sdp_producer.py              # full sync
    python producers/sdp_producer.py --days 7     # last 7 days only
"""

import os
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from auth.token_manager import TokenManager

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("sdp_producer")

# ── Config ────────────────────────────────────────────────────────────────────
SDP_BASE_URL   = "https://sdpondemand.manageengine.com/api/v3"
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC    = "sdp.tickets.raw"
PAGE_SIZE      = 100   # max allowed by SDP API


def _delivery_cb(err, msg):
    if err:
        log.error("Kafka delivery failed: %s", err)


def build_list_query(start_index: int, row_count: int, from_date: str | None) -> dict:
    """Build the SDP list_info query payload."""
    list_info = {
        "start_index": start_index,
        "row_count":   row_count,
        "sort_field":  "created_time",
        "sort_order":  "desc",
        "get_total_count": True,
    }
    if from_date:
        list_info["search_criteria"] = [
            {
                "field":     "created_time",
                "condition": "greater than",
                "value":     from_date,
                "logical_operator": "AND",
            }
        ]
    return {"list_info": list_info}


def fetch_page(session: requests.Session, token: str, start: int, from_date: str | None) -> dict:
    payload = build_list_query(start, PAGE_SIZE, from_date)
    resp = session.get(
        f"{SDP_BASE_URL}/requests",
        headers={
            "Authorization": f"Zoho-oauthtoken {token}",
            "Accept":        "application/vnd.manageengine.sdp.v3+json",
        },
        params={"input_data": json.dumps(payload)},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def normalise_ticket(raw: dict) -> dict:
    """Flatten the nested SDP ticket structure into a clean flat dict."""
    def _text(obj):
        """Extract .name or .display_value from nested objects."""
        if isinstance(obj, dict):
            return obj.get("name") or obj.get("display_value") or obj.get("id")
        return obj

    def _epoch_to_iso(obj):
        if isinstance(obj, dict) and "value" in obj:
            try:
                return datetime.fromtimestamp(int(obj["value"]) / 1000).isoformat()
            except Exception:
                return None
        return None

    return {
        "ticket_id":        raw.get("id"),
        "subject":          raw.get("subject"),
        "status":           _text(raw.get("status")),
        "priority":         _text(raw.get("priority")),
        "urgency":          _text(raw.get("urgency")),
        "impact":           _text(raw.get("impact")),
        "category":         _text(raw.get("category")),
        "subcategory":      _text(raw.get("subcategory")),
        "group":            _text(raw.get("group")),
        "technician":       _text(raw.get("technician")),
        "requester":        _text(raw.get("requester")),
        "site":             _text(raw.get("site")),
        "department":       _text(raw.get("department")),
        "mode":             _text(raw.get("mode")),
        "created_time":     _epoch_to_iso(raw.get("created_time")),
        "due_by_time":      _epoch_to_iso(raw.get("due_by_time")),
        "resolved_time":    _epoch_to_iso(raw.get("resolved_time")),
        "closed_time":      _epoch_to_iso(raw.get("closed_time")),
        "first_response_time": _epoch_to_iso(raw.get("first_response_due_by_time")),
        "is_overdue":       raw.get("is_overdue", False),
        "is_first_response_overdue": raw.get("is_first_response_overdue", False),
        "sla_violated":     raw.get("is_overdue", False),
        "source_raw":       json.dumps(raw),   # keep full payload for reprocessing
        "_ingested_at":     datetime.utcnow().isoformat(),
    }


def run(days: int | None = None):
    tm      = TokenManager()
    token   = tm.get_access_token()

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id":         "sdp-ticket-producer",
        "acks":              "all",
    })

    from_date = None
    if days:
        cutoff    = datetime.utcnow() - timedelta(days=days)
        from_date = cutoff.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        log.info("Fetching tickets since %s", from_date)
    else:
        log.info("Fetching ALL tickets (full sync)")

    session      = requests.Session()
    start_index  = 1
    total_fetched = 0
    total_count  = None

    while True:
        # Refresh token before each page (it might expire mid-run)
        token = tm.get_access_token()

        try:
            data = fetch_page(session, token, start_index, from_date)
        except requests.HTTPError as e:
            log.error("HTTP error fetching page starting at %d: %s", start_index, e)
            log.error("Response body: %s", e.response.text if e.response is not None else "N/A")
            break

        tickets      = data.get("requests", [])
        response_info = data.get("response_status", {})

        if total_count is None:
            total_count = data.get("list_info", {}).get("total_count", "?")
            log.info("Total tickets to fetch: %s", total_count)

        if not tickets:
            break

        for raw_ticket in tickets:
            flat = normalise_ticket(raw_ticket)
            producer.produce(
                topic     = KAFKA_TOPIC,
                key       = str(flat["ticket_id"]),
                value     = json.dumps(flat),
                callback  = _delivery_cb,
            )
            producer.poll(0)
            total_fetched += 1

        log.info("Fetched page starting at %d | published %d / %s so far",
                 start_index, total_fetched, total_count)

        if len(tickets) < PAGE_SIZE:
            break   # last page

        start_index += PAGE_SIZE
        time.sleep(0.2)  # be polite to the API

    producer.flush()
    log.info("✅  Done. Total tickets published to Kafka: %d", total_fetched)


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SDP → Kafka producer")
    parser.add_argument(
        "--days", type=int, default=None,
        help="Only fetch tickets from the last N days (default: full sync)"
    )
    args = parser.parse_args()
    run(days=args.days)
