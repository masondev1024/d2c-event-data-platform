"""Generate the recovery drill's source events through the isolated raffle writer."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import pbkdf2_hmac
from http.cookiejar import CookieJar
import json
import re
import secrets
from threading import Event
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, HTTPRedirectHandler, ProxyHandler, Request, build_opener


USER_COUNT = 100
ITEM_COUNT = 100
WORKERS = 4
EVENT_TYPE = "raffle.entry.accepted.v1"


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _ResponseError(ValueError):
    """A diagnostic safe to report without including the response body."""


def _seed_items_and_users(stack, usernames, titles, password):
    # Werkzeug accepts this PBKDF2 encoding. The host runner needs only stdlib.
    salt = secrets.token_hex(16)
    digest = pbkdf2_hmac("sha256", password.encode(), salt.encode(), 1_000_000).hex()
    password_hash = f"pbkdf2:sha256:1000000${salt}${digest}"
    users = ", ".join(f"('{name}', @fixture_hash)" for name in usernames)
    items = ", ".join(f"('{title}', @fixture_end)" for title in titles)
    title_list = ", ".join(f"'{title}'" for title in titles)
    # All literals come from the generated fixture ordinals or the encoded hash.
    try:
        rows = stack.sql(f"""
            START TRANSACTION;
            SET @fixture_hash = '{password_hash}', @fixture_end = UTC_TIMESTAMP() + INTERVAL 1 DAY;
            INSERT INTO users (username, password) VALUES {users};
            INSERT INTO raffle_items (title, end_time) VALUES {items};
            SELECT id, title FROM raffle_items WHERE title IN ({title_list}) ORDER BY title;
            COMMIT;
        """)
    except Exception:
        # A SQL client may echo statements (including the verifier) on error.
        raise RuntimeError("recovery cohort fixture transaction failed") from None
    item_ids = []
    actual_titles = []
    try:
        for row in rows.splitlines():
            item_id, title = row.split("\t")
            item_ids.append(int(item_id))
            actual_titles.append(title)
    except ValueError:
        raise RuntimeError("recovery cohort fixture lookup returned invalid rows") from None
    if actual_titles != titles or len(set(item_ids)) != len(titles) or min(item_ids) <= 0:
        raise RuntimeError("recovery cohort fixture item IDs differ from the requested catalogue")
    return item_ids


def _post(client, url, payload, token):
    request = Request(url, data=json.dumps(payload).encode(), headers={
        "Content-Type": "application/json", "X-CSRFToken": token,
    })
    with client.open(request, timeout=20) as response:
        if response.status != 200:
            raise _ResponseError(f"HTTP {response.status}")
        result = json.load(response)
    if not isinstance(result, dict) or result.get("status") != "success":
        raise _ResponseError("writer did not acknowledge success")
    return result


def _apply_user(index, username, api, password, item_ids, stop):
    if stop.is_set():
        return 0
    client = build_opener(ProxyHandler({}), _NoRedirect(), HTTPCookieProcessor(CookieJar()))
    stage = "GET /login"
    accepted = 0
    try:
        with client.open(api + "/login", timeout=20) as page:
            match = re.search(rb'<meta name="csrf-token" content="([^"]+)">', page.read())
        if not match:
            raise _ResponseError("missing login CSRF token")
        stage = "POST /api/login"
        login = _post(client, api + "/api/login", {"username": username, "password": password},
                      match[1].decode("ascii"))
        token = login.get("csrf_token")
        if not isinstance(token, str) or not token:
            raise _ResponseError("missing authenticated CSRF token")
        # Start each user on a different item to spread the writer's FOR UPDATE locks.
        for offset in range(len(item_ids)):
            if stop.is_set():
                break
            item = item_ids[(index + offset) % len(item_ids)]
            stage = f"POST /api/apply item_id={item}"
            result = _post(client, api + "/api/apply", {"item_id": item}, token)
            if result.get("event_type") != EVENT_TYPE:
                raise _ResponseError("unexpected event type")
            accepted += 1
        return accepted
    except HTTPError as error:
        error.close()
        reason = f"HTTP {error.code}"
    except _ResponseError as error:
        reason = str(error)
    except Exception as error:
        reason = type(error).__name__
    stop.set()
    # A lost response can follow a committed transaction. Never retry the POST.
    raise RuntimeError(f"recovery cohort {username} {stage} failed ({reason}, "
                       f"previously acknowledged for this user={accepted})")


def seed_and_apply(stack) -> dict[str, int]:
    """On a fresh migrated writer, seed fixtures and submit every user/item pair once.

    stack.sql(statement) returns tab-separated MySQL batch output and stack.url("api")
    returns the writer base URL. After failure discard the partially applied stack.
    """
    usernames = [f"recovery_user_{i:03d}" for i in range(1, USER_COUNT + 1)]
    titles = [f"Recovery item {i:03d}" for i in range(1, ITEM_COUNT + 1)]
    try:
        api = stack.url("api").rstrip("/")
    except Exception:
        raise RuntimeError("recovery cohort writer address lookup failed") from None
    password = secrets.token_urlsafe(32)
    item_ids = _seed_items_and_users(stack, usernames, titles, password)
    stop = Event()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        try:
            jobs = [pool.submit(_apply_user, index, name, api, password, item_ids, stop)
                    for index, name in enumerate(usernames)]
            accepted = sum(job.result() for job in jobs)
        finally:
            stop.set()

    # The writer commits the entry and event together. Verify the stored pairs as
    # well as the client acknowledgements before passing the source to the relay.
    try:
        rows = stack.sql(f"""
            SELECT (SELECT COUNT(*) FROM raffle_entries),
                   (SELECT COUNT(*) FROM raffle_outbox_events),
                   COUNT(*) FROM raffle_entries e JOIN raffle_outbox_events o
                     ON o.aggregate_id = e.id AND o.aggregate_type = 'raffle_entry'
                    AND o.event_type = '{EVENT_TYPE}';
        """)
        entries, events, matched = map(int, rows.split())
    except Exception:
        raise RuntimeError("recovery cohort source counts unavailable") from None
    if not (len(usernames) * len(item_ids) == accepted == entries == events == matched):
        raise RuntimeError(f"recovery cohort reconciliation failed: {accepted} acknowledged applies, "
                           f"{entries} entries, {events} outbox events, {matched} matching events")
    return {"users": len(usernames), "items": len(item_ids), "successful_applies": accepted,
            "writer_entries": entries, "writer_outbox_events": events}
