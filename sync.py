# sync.py
import pathlib
from dotenv import load_dotenv
import os, hashlib, json, requests, datetime
from dateutil import tz
from dateutil.parser import isoparse
from dateutil.rrule import rrulestr
from icalendar import Calendar, Event
from googleapiclient.discovery import build
from google.oauth2 import service_account
from pytz import timezone, UTC

load_dotenv(dotenv_path=pathlib.Path(__file__).with_name(".env"))

STATE_FILE = os.getenv("STATE_FILE", ".state.json")


def _load_state():
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(d):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f)
    os.replace(tmp, STATE_FILE)


# --- Config ---
ICS_URL = os.getenv("ICS_URL")
TARGET_CALENDAR_ID = os.getenv("TARGET_CALENDAR_ID")
TIMEZONE = os.getenv("TIMEZONE", "Europe/Paris")
PAST_DAYS = int(os.getenv("PAST_DAYS", "30"))
TZ = timezone(TIMEZONE)
WINDOWS_TZMAP = {
    "Romance Standard Time": "Europe/Paris",
    "W. Europe Standard Time": "Europe/Berlin",
    "Central European Standard Time": "Europe/Warsaw",
    "FLE Standard Time": "Europe/Helsinki",
}


# --- Auth ---
def gcal_service():
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def _format_recur_line(kind, is_all_day, dt_list_dt, tzid):
    """
    kind: 'EXDATE' | 'RDATE'
    dt_list_dt: список datetime.date или datetime.datetime (в ТВОЕЙ TZ)
    tzid: строка IANA таймзоны (например, 'Europe/Paris')
    Возвращает строку RFC5545 для поля recurrence, например:
      EXDATE;VALUE=DATE:20250904,20250918
      EXDATE;TZID=Europe/Paris:20250904T140000,20250918T140000
    """
    if is_all_day:
        vals = ",".join(
            dt.strftime("%Y%m%d") if hasattr(dt, "strftime") else str(dt)
            for dt in dt_list_dt
        )
        return f"{kind};VALUE=DATE:{vals}"
    else:
        # локальные времена (без Z и без офсета) + TZID=...
        vals = ",".join(dt.strftime("%Y%m%dT%H%M%S") for dt in dt_list_dt)
        return f"{kind};TZID={tzid}:{vals}"


def _fmt_until_value(val):
    # val может быть date, datetime или vDDD
    if isinstance(val, datetime.datetime):
        # в UTC и компакт
        v = val.astimezone(UTC)
        return v.strftime("%Y%m%dT%H%M%SZ")
    if isinstance(val, datetime.date):
        # трактуем как 00:00:00Z
        return datetime.datetime(val.year, val.month, val.day, tzinfo=UTC).strftime(
            "%Y%m%dT%H%M%SZ"
        )
    # fallback
    s = str(val)
    # если уже вида 20260305T120000Z — оставим
    return s


# --- Helpers ---
def normalize_dt(value, src_tzid=None, target_tz=None):
    """
    Return (is_all_day, aware_dt_in_target_tz).
    Если target_tz не задан, используем глобальный TZ (Europe/Paris).
    """
    target = (
        timezone(map_tzid(target_tz))
        if isinstance(target_tz, str)
        else (target_tz or TZ)
    )

    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        # all-day
        return True, datetime.datetime.combine(value, datetime.time(0, 0)).replace(
            tzinfo=target
        )

    dt = value
    if dt.tzinfo is None:
        src = timezone(map_tzid(src_tzid)) if src_tzid else target
        dt = src.localize(dt)
    return False, dt.astimezone(target)

def event_fingerprint(
    summary, description, loc, start_str, end_str, rrule, exdates, rdates
):
    payload = json.dumps(
        {
            "s": summary or "",
            "d": description or "",
            "l": loc or "",
            "st": start_str,
            "et": end_str,
            "rr": rrule or "",
            "ex": exdates or [],
            "rd": rdates or [],
        },
        sort_keys=True,
    )
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def gcal_list_existing(service):
    """Return dict: outlook_uid -> (gcal_event) for future events that we created earlier (identified via private extendedProperties)."""
    now = datetime.datetime.now(UTC).isoformat()
    items = {}
    page_token = None
    while True:
        resp = (
            service.events()
            .list(
                calendarId=TARGET_CALENDAR_ID,
                timeMin=now,
                singleEvents=False,
                maxResults=2500,
                pageToken=page_token,
                privateExtendedProperty="src=outlook_ics",
            )
            .execute()
        )
        for ev in resp.get("items", []):
            props = ev.get("extendedProperties", {}).get("private", {})
            uid = props.get("outlook_uid")
            if uid:
                items[uid] = ev
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return items


def map_tzid(src_tzid: str | None) -> str:
    if not src_tzid:
        return TIMEZONE
    return WINDOWS_TZMAP.get(src_tzid, src_tzid)


def build_rrule_string(rrule_dict):
    parts = []
    for k in sorted(rrule_dict.keys(), key=str.upper):
        key = k.upper()
        vals = rrule_dict[k]
        out_vals = []
        for v in vals:
            if key == "UNTIL":
                out_vals.append(_fmt_until_value(v))
            elif key == "COUNT":
                out_vals.append(str(int(v)))
            else:
                out_vals.append(str(v).upper())
        parts.append(f"{key}={','.join(out_vals)}")
    return ";".join(parts)


def to_gcal_resource(vevent: Event):
    # UID (+ уникальность для overridden instances)
    base_uid = str(vevent.get("UID"))
    recur_id_prop = vevent.get("RECURRENCE-ID")
    if recur_id_prop:
        rid_tzid = recur_id_prop.params.get("TZID")
        _, rid_dt = normalize_dt(recur_id_prop.dt, rid_tzid)
        uid = f"{base_uid}::{rid_dt.isoformat()}"
    else:
        uid = base_uid

    summary = str(vevent.get("SUMMARY", "")) or ""
    description = str(vevent.get("DESCRIPTION", "")) or ""
    location = str(vevent.get("LOCATION", "")) or ""

    # DTSTART/DTEND (с учётом исходного TZID события)
    dtstart_prop = vevent.get("DTSTART")
    dtstart_tzid_raw = (
        dtstart_prop.params.get("TZID") if hasattr(dtstart_prop, "params") else None
    )
    event_tzid = map_tzid(dtstart_tzid_raw)  # <-- IANA для этого события
    event_tz = timezone(event_tzid)

    allday_start, start_dt_local = normalize_dt(
        dtstart_prop.dt, dtstart_tzid_raw, event_tz
    )

    dtend_prop = vevent.get("DTEND")
    if dtend_prop:
        dtend_tzid_raw = dtend_prop.params.get("TZID") if hasattr(dtend_prop, "params") else None
        allday_end, end_dt_local = normalize_dt(dtend_prop.dt, dtend_tzid_raw, event_tz)
    else:
        duration = vevent.get("DURATION")
        if duration:
            end_dt_local = (start_dt_local + duration.dt).astimezone(event_tz)
            allday_end = allday_start
        else:
            end_dt_local = (start_dt_local + (datetime.timedelta(days=1) if allday_start else datetime.timedelta(hours=1))).astimezone(event_tz)
            allday_end = allday_start

    if allday_start:
        start = {"date": start_dt_local.date().isoformat()}
    else:
        start = {"dateTime": start_dt_local.isoformat(), "timeZone": event_tzid}

    if allday_end:
        end = {"date": end_dt_local.date().isoformat()}
    else:
        end = {"dateTime": end_dt_local.isoformat(), "timeZone": event_tzid}

    # RRULE
    rrule = vevent.get("RRULE")
    g_rrule = None
    if rrule:
        g_rrule = build_rrule_string(rrule)

        # EXDATE (может быть объект или список объектов)
    exdates_dt = []
    ex_prop = vevent.get("EXDATE")
    if ex_prop:
        ex_list = ex_prop if isinstance(ex_prop, list) else [ex_prop]
        for ex in ex_list:
            ex_tzid = ex.params.get("TZID") if hasattr(ex, "params") else None
            for comp in getattr(ex, "dts", []):
                _allday_x, ex_dt_local = normalize_dt(comp.dt, ex_tzid, event_tz)
                exdates_dt.append(
                    ex_dt_local if not allday_start else ex_dt_local.date()
                )

    rdates_dt = []
    r_prop = vevent.get("RDATE")
    if r_prop:
        r_list = r_prop if isinstance(r_prop, list) else [r_prop]
        for r in r_list:
            r_tzid = r.params.get("TZID") if hasattr(r, "params") else None
            for comp in getattr(r, "dts", []):
                _allday_r, r_dt_local = normalize_dt(comp.dt, r_tzid, event_tz)
                rdates_dt.append(r_dt_local if not allday_start else r_dt_local.date())

    resource = {
        "summary": summary,
        "description": description,
        "location": location,
        "start": start,
        "end": end,
        "extendedProperties": {"private": {"src": "outlook_ics", "outlook_uid": uid}},
    }

    # recurrence собираем ТОЛЬКО для master (без RECURRENCE-ID)
    recurrence_lines = []
    if not recur_id_prop:
        if g_rrule:
            recurrence_lines.append(f"RRULE:{g_rrule}")
        if exdates_dt:
            recurrence_lines.append(_format_recur_line("EXDATE", "date" in start, exdates_dt, event_tzid))
        if rdates_dt:
            recurrence_lines.append(_format_recur_line("RDATE", "date" in start, rdates_dt, event_tzid))
        if recurrence_lines:
            resource["recurrence"] = recurrence_lines

        # fingerprint
    fp = event_fingerprint(
        resource["summary"],
        resource["description"],
        resource["location"],
        json.dumps(resource["start"]),
        json.dumps(resource["end"]),
        g_rrule,
        # сериализуем для отпечатка
        [dt.isoformat() if hasattr(dt, "isoformat") else str(dt) for dt in exdates_dt],
        [dt.isoformat() if hasattr(dt, "isoformat") else str(dt) for dt in rdates_dt],
    )

    resource["extendedProperties"]["private"]["fp"] = fp
    return uid, resource


def main():
    assert ICS_URL and TARGET_CALENDAR_ID, "ICS_URL and TARGET_CALENDAR_ID must be set"
    svc = gcal_service()

    # --- 1) Load prev state ---
    state = _load_state()
    headers = {}
    if et := state.get("etag"):
        headers["If-None-Match"] = et
    if lm := state.get("last_modified"):
        headers["If-Modified-Since"] = lm

    # --- 2) Fetch ICS (with conditional headers) ---
    r = requests.get(ICS_URL, headers=headers, timeout=30)
    if r.status_code == 304:
        print("ICS not modified (304) -> exit")
        return
    r.raise_for_status()

    content = r.content
    curr_hash = hashlib.md5(content).hexdigest()
    if curr_hash == state.get("hash"):
        print("ICS hash unchanged -> exit")
        return

    # Save new state
    new_state = {
        "etag": r.headers.get("ETag"),
        "last_modified": r.headers.get("Last-Modified"),
        "hash": curr_hash,
    }
    _save_state(new_state)

    # --- 3) Parse ICS ---
    cal = Calendar.from_ical(content)
    now = datetime.datetime.now(TZ)
    cutoff = now - datetime.timedelta(days=PAST_DAYS)

    src = {}
    for comp in cal.walk("VEVENT"):
        dtstart = comp.get("DTSTART")
        if not dtstart:
            continue
        dtstart_tzid = (
            dtstart.params.get("TZID") if hasattr(dtstart, "params") else None
        )
        _allday, start_dt = normalize_dt(dtstart.dt, dtstart_tzid)

        has_rrule = comp.get("RRULE") is not None
        has_recur_id = comp.get("RECURRENCE-ID") is not None

        keep = False
        if has_rrule and not has_recur_id:
            keep = True  # master серии держим всегда
        else:
            keep = start_dt >= cutoff

        if not keep:
            continue

        uid, resource = to_gcal_resource(comp)
        src[uid] = resource

    # --- 4) Get existing events in target calendar ---
    existing = gcal_list_existing(svc)

    # --- 5) Upsert ---
    for uid, res in src.items():
        try:
            ex = existing.get(uid)
            if ex:
                old_fp = ex.get("extendedProperties", {}).get("private", {}).get("fp")
                if old_fp != res["extendedProperties"]["private"]["fp"]:
                    if "recurrence" in res:
                        print("PATCH RECURRENCE ->", res["recurrence"])
                    svc.events().patch(
                        calendarId=TARGET_CALENDAR_ID, eventId=ex["id"], body=res
                    ).execute()
            else:
                if "recurrence" in res:
                    print("INSERT RECURRENCE ->", res["recurrence"])
                svc.events().insert(calendarId=TARGET_CALENDAR_ID, body=res).execute()
        except Exception as e:
            print("FAILED UID:", uid)
            if "recurrence" in res:
                print("RECURRENCE LINES:", res.get("recurrence"))
            raise

    # --- 6) Delete disappeared events ---
    for uid, ex in existing.items():
        if uid not in src:
            svc.events().delete(
                calendarId=TARGET_CALENDAR_ID, eventId=ex["id"]
            ).execute()


if __name__ == "__main__":
    main()
