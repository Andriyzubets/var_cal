# block.py — обратное направление: занятость из календарей Марины -> инвайты "Busy" в рабочий Outlook
import datetime, fnmatch, hashlib, json, os, smtplib, sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dateutil.parser import isoparse
from icalendar import Calendar, Event, vCalAddress
from pytz import UTC

from sync import gcal_service, TZ  # sync.py уже грузит .env

# --- Config ---
MARINAILS_CAL = os.getenv("BLOCK_MARINAILS_CAL")
MARINA_CAL = os.getenv("BLOCK_MARINA_CAL")
KIDS_CAL = os.getenv("BLOCK_KIDS_CAL")  # опционально
BLOCK_TO = os.getenv("BLOCK_TO")  # рабочий email (Outlook)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = (os.getenv("SMTP_PASSWORD") or "").replace(" ", "")  # Gmail App Password
HORIZON_DAYS = int(os.getenv("BLOCK_HORIZON_DAYS", "45"))
STATE_FILE = os.getenv("BLOCK_STATE_FILE", ".block_state.json")
SUMMARY = os.getenv("BLOCK_SUMMARY", "Busy")
# события с такими названиями (без учёта регистра, * — любой хвост) не блокируют время
IGNORE_TITLES = [
    t.strip().lower()
    for t in os.getenv("BLOCK_IGNORE_TITLES", "Sv,Каникулы,Санта луазир,Gym*").split(",")
    if t.strip()
]

# weekday(): Пн=0 ... Вс=6. Окна в локальном времени TZ: (начало, конец, конец триггера).
# Окно срабатывает, только если в [начало, конец триггера) есть событие;
# тогда блокируется всё окно (whole_window) или только пересечения событий с ним.
MARINAILS_WINDOWS = {
    1: [("15:15", "16:15", "16:00")],  # Вт
    2: [("11:30", "12:30", "12:00")],  # Ср
    4: [("15:00", "16:00", "15:30")],  # Пт
}
MARINA_WINDOWS = {d: [("08:00", "18:00", "18:00")] for d in range(5)}  # Пн–Пт
KIDS_WINDOWS = MARINA_WINDOWS


# --- Google ---
def list_events(svc, calendar_id, time_min, time_max):
    items, page_token = [], None
    while True:
        resp = (
            svc.events()
            .list(
                calendarId=calendar_id,
                timeMin=time_min.isoformat(),
                timeMax=time_max.isoformat(),
                singleEvents=True,
                maxResults=2500,
                pageToken=page_token,
            )
            .execute()
        )
        items.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            return items


def event_span(ev):
    """(start, end) в TZ для событий со временем; None для all-day/отменённых/"свободно"/игнор-списка."""
    if ev.get("status") == "cancelled" or ev.get("transparency") == "transparent":
        return None
    title = (ev.get("summary") or "").strip().lower()
    if any(fnmatch.fnmatchcase(title, pat) for pat in IGNORE_TITLES):
        return None
    st, en = ev.get("start", {}), ev.get("end", {})
    if "dateTime" not in st or "dateTime" not in en:
        return None
    return isoparse(st["dateTime"]).astimezone(TZ), isoparse(en["dateTime"]).astimezone(TZ)


# --- Интервалы ---
def day_windows(first_day, last_day, rules):
    day = first_day
    while day <= last_day:
        for times in rules.get(day.weekday(), []):
            yield tuple(
                TZ.localize(datetime.datetime.combine(day, datetime.time.fromisoformat(t))) for t in times
            )
        day += datetime.timedelta(days=1)


def busy_intervals(events, rules, first_day, last_day, src, whole_window=False):
    spans = [sp for sp in map(event_span, events) if sp]
    out = []
    for ws, we, wt in day_windows(first_day, last_day, rules):
        if not any(es < wt and ee > ws for es, ee in spans):
            continue  # в триггерной части окна пусто — окно пропускаем
        if whole_window:
            out.append((ws, we, src))
            continue
        for es, ee in spans:
            s, e = max(es, ws), min(ee, we)
            if s < e:
                out.append((s, e, src))
    return out


def merge(intervals):
    merged = []
    for s, e, src in sorted(intervals):
        if merged and s <= merged[-1][1]:
            ps, pe, psrc = merged[-1]
            srcs = psrc if src in psrc.split(", ") else f"{psrc}, {src}"
            merged[-1] = (ps, max(pe, e), srcs)
        else:
            merged.append((s, e, src))
    return merged


def block_uid(s, e):
    key = f"{s.astimezone(UTC).isoformat()}|{e.astimezone(UTC).isoformat()}"
    return hashlib.sha1(key.encode()).hexdigest()[:20] + "@var-cal"


# --- iMIP ---
def build_ics(method, uid, s, e, src, seq):
    cal = Calendar()
    cal.add("prodid", "-//var_cal//block//EN")
    cal.add("version", "2.0")
    cal.add("method", method)
    ev = Event()
    ev.add("uid", uid)
    ev.add("dtstamp", datetime.datetime.now(UTC))
    ev.add("dtstart", s.astimezone(UTC))
    ev.add("dtend", e.astimezone(UTC))
    ev.add("summary", SUMMARY)
    ev.add("description", f"Auto-block (var_cal): {src}")
    ev.add("sequence", seq)
    ev.add("status", "CANCELLED" if method == "CANCEL" else "CONFIRMED")
    ev.add("transp", "OPAQUE")
    ev.add("X-MICROSOFT-CDO-BUSYSTATUS", "BUSY")
    ev.add("X-MICROSOFT-CDO-INTENDEDSTATUS", "BUSY")
    organizer = vCalAddress(f"mailto:{SMTP_USER}")
    ev.add("organizer", organizer)
    attendee = vCalAddress(f"mailto:{BLOCK_TO}")
    attendee.params["ROLE"] = "REQ-PARTICIPANT"
    attendee.params["PARTSTAT"] = "NEEDS-ACTION"
    attendee.params["RSVP"] = "FALSE"
    ev.add("attendee", attendee)
    cal.add_component(ev)
    return cal.to_ical().decode("utf-8")


def build_message(method, uid, s, e, src, seq):
    when = f"{s.strftime('%a %d.%m %H:%M')}–{e.strftime('%H:%M')}"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"{'Cancelled: ' if method == 'CANCEL' else ''}{SUMMARY} {when}"
    msg["From"] = SMTP_USER
    msg["To"] = BLOCK_TO
    msg["Content-Class"] = "urn:content-classes:calendarmessage"
    msg.attach(MIMEText(f"{SUMMARY} {when} ({src})", "plain", "utf-8"))
    part = MIMEText(build_ics(method, uid, s, e, src, seq), "calendar", "utf-8")
    part.set_param("method", method)
    msg.attach(part)
    return msg


def _load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"blocks": {}}


def _save_state(d):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, indent=1)
    os.replace(tmp, STATE_FILE)


def main():
    dry_run = "--dry-run" in sys.argv
    assert MARINAILS_CAL and MARINA_CAL, "BLOCK_MARINAILS_CAL and BLOCK_MARINA_CAL must be set"
    if not dry_run:
        assert BLOCK_TO and SMTP_USER and SMTP_PASSWORD, "BLOCK_TO, SMTP_USER, SMTP_PASSWORD must be set"

    now = datetime.datetime.now(TZ)
    first_day = now.date()
    last_day = first_day + datetime.timedelta(days=HORIZON_DAYS)
    horizon_end = TZ.localize(datetime.datetime.combine(last_day + datetime.timedelta(days=1), datetime.time()))
    day_start = TZ.localize(datetime.datetime.combine(first_day, datetime.time()))

    # Любая ошибка чтения — исключение до отправки отмен, чтобы не снести все блоки
    svc = gcal_service()
    sources = [(MARINAILS_CAL, MARINAILS_WINDOWS, "Marinails", True), (MARINA_CAL, MARINA_WINDOWS, "Marina", False)]
    if KIDS_CAL:
        sources.append((KIDS_CAL, KIDS_WINDOWS, "Kids", False))
    intervals = []
    for cal_id, rules, src, whole in sources:
        events = list_events(svc, cal_id, day_start, horizon_end)
        intervals += busy_intervals(events, rules, first_day, last_day, src, whole)
    desired = {block_uid(s, e): (s, e, src) for s, e, src in merge(intervals) if e > now}

    state = _load_state()
    blocks = state.setdefault("blocks", {})
    smtp = None

    def send(msg):
        nonlocal smtp
        if smtp is None:
            smtp = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
            smtp.starttls()
            # только PLAIN: при фолбэке на LOGIN Gmail рвёт соединение и прячет 535 BadCredentials
            smtp.user, smtp.password = SMTP_USER, SMTP_PASSWORD
            smtp.auth("PLAIN", smtp.auth_plain)
        smtp.send_message(msg)

    try:
        for uid, b in list(blocks.items()):
            if uid in desired:
                continue
            s, e = isoparse(b["start"]), isoparse(b["end"])
            if e <= now:
                del blocks[uid]  # прошло — просто забываем
                continue
            if s >= horizon_end:
                continue
            print(f"CANCEL {s:%Y-%m-%d %H:%M}–{e:%H:%M} ({b['src']})")
            if not dry_run:
                send(build_message("CANCEL", uid, s, e, b["src"], b.get("seq", 0) + 1))
                del blocks[uid]
                _save_state(state)

        for uid, (s, e, src) in sorted(desired.items(), key=lambda kv: kv[1][0]):
            if uid in blocks:
                continue
            print(f"BLOCK  {s:%Y-%m-%d %H:%M}–{e:%H:%M} ({src})")
            if not dry_run:
                send(build_message("REQUEST", uid, s, e, src, 0))
                blocks[uid] = {"start": s.isoformat(), "end": e.isoformat(), "src": src, "seq": 0}
                _save_state(state)

        if not dry_run:
            _save_state(state)
    finally:
        if smtp is not None:
            smtp.quit()


if __name__ == "__main__":
    main()
