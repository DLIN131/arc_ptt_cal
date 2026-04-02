#!/usr/bin/env python3
"""Arcaea PTT Calculator Server — MongoDB + JWT Auth."""

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

# ── Optional deps (graceful degradation if not installed) ─────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import bcrypt
    import jwt
    from bson import ObjectId
    from pymongo import MongoClient
    HAS_MONGO = True
except ImportError:
    HAS_MONGO = False

# ── Config ────────────────────────────────────────────────────────────────────
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "10000"))
INDEX_FILE = "deepseek_html_20260402_d78591.html"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MONGODB_URI = os.getenv("MONGODB_URI", "")
JWT_SECRET = os.getenv("JWT_SECRET", "arcaea-dev-secret-changeme")
JWT_ALGORITHM = "HS256"
JWT_EXPIRES_DAYS = 7

# ── Song sync sources ─────────────────────────────────────────────────────────
SYNC_SOURCES = [
    {
        "name": "ArcWiki template raw json via r.jina.ai",
        "url": "https://r.jina.ai/http://arcwiki.mcd.blue/index.php?title=Template:ChartConstant.json%26action=raw",
        "parser": "template",
    },
    {
        "name": "ArcWiki constants table via r.jina.ai",
        "url": (
            "https://r.jina.ai/http://arcwiki.mcd.blue/index.php"
            "?mobileaction=toggle_view_desktop"
            "&title=%E5%AE%9A%E6%95%B0%E8%AF%A6%E8%A1%A8"
            "&variant=zh-tw"
        ),
        "parser": "table",
    },
]

KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_]*$", re.IGNORECASE)
CONSTANT_RE = re.compile(r"^constant\s+(\d+(?:\.\d+)?)$", re.IGNORECASE)
TABLE_LINE_RE = re.compile(r"\|")
NUM_RE = re.compile(r"\d+(?:\.\d+)?")
DIFF_ORDER = ["PST", "PRS", "FTR", "BYD", "ETR"]

# ── MongoDB singleton ─────────────────────────────────────────────────────────
_mongo_client = None
_db = None


def get_db():
    global _mongo_client, _db
    if not HAS_MONGO or not MONGODB_URI:
        return None
    if _db is None:
        _mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        _db = _mongo_client["arc_ptt_cal"]
        _db.users.create_index("username", unique=True)
        _db.records.create_index("userId")
        _db.songs.create_index([("songName", 1), ("difficulty", 1)], unique=True)
    return _db


# ── Auth helpers ──────────────────────────────────────────────────────────────
def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def create_token(user_id: str, username: str) -> str:
    payload = {
        "sub": user_id,
        "username": username,
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRES_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str):
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except Exception:
        return None


# ── Song parsing utilities ────────────────────────────────────────────────────
def normalize_difficulty(token: str):
    t = (token or "").strip().lower()
    if not t:
        return None
    if re.search(r"\b(pst|past|0)\b", t):
        return "PST"
    if re.search(r"\b(prs|present|1)\b", t):
        return "PRS"
    if re.search(r"\b(ftr|future|2)\b", t):
        return "FTR"
    if re.search(r"\b(byd|beyond|3)\b", t):
        return "BYD"
    if re.search(r"\b(etr|eternal|4)\b", t):
        return "ETR"
    return None


def parse_constant_value(text: str):
    m = NUM_RE.search(text or "")
    if not m:
        return None
    try:
        value = float(m.group(0))
    except ValueError:
        return None
    if value < 0 or value > 13:
        return None
    return round(value, 2)


def dedupe_and_normalize(charts):
    dedup = {}
    for row in charts:
        song_name = str(row.get("songName", "")).strip()
        diff = normalize_difficulty(str(row.get("difficulty", "")))
        const = parse_constant_value(str(row.get("constant", "")))
        if not song_name or not diff or const is None:
            continue
        dedup[(song_name.lower(), diff)] = {
            "songName": song_name,
            "difficulty": diff,
            "constant": const,
        }
    sorted_rows = sorted(dedup.values(), key=lambda r: (r["songName"].lower(), r["difficulty"]))
    for idx, row in enumerate(sorted_rows, start=1):
        row["id"] = idx
    return sorted_rows


def parse_template_text(text: str):
    # Try to find the JSON block inside Jina's response
    m = re.search(r'```(?:json)?(.*?)```', text, re.DOTALL)
    if m:
        js_text = m.group(1)
    else:
        # Fallback: find the first { to the last }
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            js_text = text[start:end+1]
        else:
            return []
            
    try:
        data = json.loads(js_text)
    except Exception as e:
        print("JSON parse error in template:", e)
        return []
    
    charts = []
    for song_id, slots in data.items():
        if not isinstance(slots, list):
            continue
        for i, val in enumerate(slots):
            if i >= len(DIFF_ORDER):
                break
            if not isinstance(val, dict):
                continue
            constant = val.get("constant")
            if constant is not None:
                charts.append({
                    "songName": str(song_id),
                    "difficulty": DIFF_ORDER[i],
                    "constant": float(constant)
                })
    return charts

def parse_table_text(text: str):
    charts = []
    for line in (text or "").splitlines():
        if not TABLE_LINE_RE.search(line):
            continue
        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 6:
            continue
        song_name = cols[1]
        diff = normalize_difficulty(cols[3])
        const = parse_constant_value(cols[5])
        if not song_name or not diff or const is None:
            continue
        charts.append({"songName": song_name, "difficulty": diff, "constant": const})
    return charts


def fetch_text(url: str, timeout: float = 45.0):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 Arcaea-PTT-Sync/1.0",
            "Accept": "text/plain,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def sync_charts_from_sources():
    errors = []
    for source in SYNC_SOURCES:
        try:
            text = fetch_text(source["url"])
            raw = (
                parse_template_text(text)
                if source["parser"] == "template"
                else parse_table_text(text)
            )
            charts = dedupe_and_normalize(raw)
            if len(charts) < 200:
                raise RuntimeError(f"Too few charts parsed ({len(charts)})")
            return {
                "ok": True,
                "source": source["name"],
                "count": len(charts),
                "charts": charts,
            }
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{source['name']} -> {exc}")
    return {"ok": False, "error": "all_sources_failed", "details": errors, "charts": []}


# ── Request handler ───────────────────────────────────────────────────────────
class AppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    # ── CORS ──────────────────────────────────────────────────────────────────
    def _add_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type,Authorization")

    def do_OPTIONS(self):  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._add_cors()
        self.end_headers()

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self._add_cors()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length))
        except Exception:
            return {}

    def _get_user(self):
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        return decode_token(auth[7:])

    def _require_user(self):
        user = self._get_user()
        if not user:
            self._send_json({"error": "Unauthorized"}, HTTPStatus.UNAUTHORIZED)
        return user

    def _send_html(self):
        path = os.path.join(BASE_DIR, INDEX_FILE)
        if not os.path.isfile(path):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(HTTPStatus.OK)
        self._add_cors()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _no_db(self):
        self._send_json({"error": "Database unavailable"}, HTTPStatus.SERVICE_UNAVAILABLE)

    # ── Routing ───────────────────────────────────────────────────────────────
    def do_GET(self):  # noqa: N802
        p = urllib.parse.urlsplit(self.path).path
        if p == "/healthz":
            self.send_response(HTTPStatus.OK)
            self._add_cors()
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        elif p in ("/", "/index.html", f"/{INDEX_FILE}"):
            self._send_html()
        elif p == "/api/songs/sync":
            self._songs_sync()
        elif p == "/api/songs":
            self._songs_get()
        elif p == "/api/records":
            self._records_get()
        else:
            super().do_GET()

    def do_POST(self):  # noqa: N802
        p = urllib.parse.urlsplit(self.path).path
        if p == "/api/auth/register":
            self._auth_register()
        elif p == "/api/auth/login":
            self._auth_login()
        elif p == "/api/records":
            self._records_post()
        else:
            self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)

    def do_DELETE(self):  # noqa: N802
        p = urllib.parse.urlsplit(self.path).path
        if p == "/api/records":
            self._records_delete_all()
        elif p.startswith("/api/records/"):
            self._records_delete_one(p[len("/api/records/"):])
        else:
            self._send_json({"error": "Not found"}, HTTPStatus.NOT_FOUND)

    # ── Auth endpoints ────────────────────────────────────────────────────────
    def _auth_register(self):
        if not HAS_MONGO:
            self._send_json({"error": "Auth not available: install dependencies"}, HTTPStatus.SERVICE_UNAVAILABLE)
            return
        db = get_db()
        if db is None:
            self._no_db(); return

        body = self._read_body()
        username = str(body.get("username", "")).strip()
        password = str(body.get("password", "")).strip()

        if not username or not password:
            self._send_json({"error": "username 和 password 為必填"}, HTTPStatus.BAD_REQUEST)
            return
        if len(username) < 3 or len(username) > 32:
            self._send_json({"error": "username 須為 3–32 個字元"}, HTTPStatus.BAD_REQUEST)
            return
        if len(password) < 6:
            self._send_json({"error": "password 至少 6 個字元"}, HTTPStatus.BAD_REQUEST)
            return

        try:
            result = db.users.insert_one({
                "username": username,
                "password": hash_password(password),
                "createdAt": datetime.now(timezone.utc),
            })
        except Exception:
            self._send_json({"error": "使用者名稱已存在"}, HTTPStatus.CONFLICT)
            return

        token = create_token(str(result.inserted_id), username)
        self._send_json({"token": token, "username": username}, HTTPStatus.CREATED)

    def _auth_login(self):
        if not HAS_MONGO:
            self._send_json({"error": "Auth not available"}, HTTPStatus.SERVICE_UNAVAILABLE)
            return
        db = get_db()
        if db is None:
            self._no_db(); return

        body = self._read_body()
        username = str(body.get("username", "")).strip()
        password = str(body.get("password", "")).strip()

        user = db.users.find_one({"username": username})
        if not user or not verify_password(password, user["password"]):
            self._send_json({"error": "帳號或密碼錯誤"}, HTTPStatus.UNAUTHORIZED)
            return

        token = create_token(str(user["_id"]), username)
        self._send_json({"token": token, "username": username})

    # ── Songs endpoints ───────────────────────────────────────────────────────
    def _songs_get(self):
        db = get_db()
        if db is None:
            self._send_json({"ok": False, "charts": [], "error": "no_db"})
            return
        try:
            songs = list(db.songs.find({}, {"_id": 0}))
            self._send_json({"ok": True, "count": len(songs), "charts": songs})
        except Exception as exc:
            self._send_json({"ok": False, "charts": [], "error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def _songs_sync(self):
        payload = sync_charts_from_sources()
        if payload.get("ok"):
            db = get_db()
            if db is not None:
                try:
                    now = datetime.now(timezone.utc)
                    for chart in payload["charts"]:
                        db.songs.update_one(
                            {"songName": chart["songName"], "difficulty": chart["difficulty"]},
                            {"$set": {**chart, "updatedAt": now}},
                            upsert=True,
                        )
                    payload["savedToDb"] = True
                except Exception as exc:
                    payload["dbError"] = str(exc)
        self._send_json(
            payload,
            status=HTTPStatus.OK if payload.get("ok") else HTTPStatus.BAD_GATEWAY,
        )

    # ── Records endpoints ─────────────────────────────────────────────────────
    def _records_get(self):
        user = self._require_user()
        if not user:
            return
        db = get_db()
        if db is None:
            self._no_db(); return
        try:
            docs = list(db.records.find(
                {"userId": user["sub"]},
                {"userId": 0},
            ))
            for d in docs:
                d["id"] = str(d.pop("_id"))
            self._send_json({"records": docs})
        except Exception as exc:
            self._send_json({"error": str(exc)}, HTTPStatus.INTERNAL_SERVER_ERROR)

    def _records_post(self):
        user = self._require_user()
        if not user:
            return
        db = get_db()
        if db is None:
            self._no_db(); return

        body = self._read_body()
        for field in ("songName", "difficulty", "constant", "score", "ptt"):
            if field not in body:
                self._send_json({"error": f"Missing field: {field}"}, HTTPStatus.BAD_REQUEST)
                return

        doc = {
            "userId": user["sub"],
            "songName": str(body["songName"]),
            "difficulty": str(body["difficulty"]),
            "constant": float(body["constant"]),
            "score": int(body["score"]),
            "ptt": float(body["ptt"]),
            "timestamp": int(body.get("timestamp", datetime.now(timezone.utc).timestamp() * 1000)),
        }
        result = db.records.insert_one(doc)
        doc["id"] = str(result.inserted_id)
        del doc["userId"]
        del doc["_id"]
        self._send_json({"record": doc}, HTTPStatus.CREATED)

    def _records_delete_one(self, record_id: str):
        user = self._require_user()
        if not user:
            return
        db = get_db()
        if db is None:
            self._no_db(); return
        try:
            oid = ObjectId(record_id)
        except Exception:
            self._send_json({"error": "Invalid record id"}, HTTPStatus.BAD_REQUEST)
            return
        result = db.records.delete_one({"_id": oid, "userId": user["sub"]})
        if result.deleted_count == 0:
            self._send_json({"error": "Record not found"}, HTTPStatus.NOT_FOUND)
            return
        self._send_json({"ok": True})

    def _records_delete_all(self):
        user = self._require_user()
        if not user:
            return
        db = get_db()
        if db is None:
            self._no_db(); return
        result = db.records.delete_many({"userId": user["sub"]})
        self._send_json({"ok": True, "deleted": result.deleted_count})

    def log_message(self, fmt, *args):
        print(f"[{self.log_date_time_string()}] {fmt % args}", flush=True)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    port = PORT
    if len(sys.argv) >= 2:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    if not MONGODB_URI:
        print("⚠  MONGODB_URI not set — auth & cloud features disabled.", flush=True)
    if not HAS_MONGO:
        print("⚠  Dependencies missing — run: pip install -r requirements.txt", flush=True)

    server = ThreadingHTTPServer((HOST, port), AppHandler)
    print(f"✅ Arcaea PTT server: http://127.0.0.1:{port}/", flush=True)
    print("   Press Ctrl+C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
