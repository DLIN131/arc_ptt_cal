#!/usr/bin/env python3
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "10000"))
INDEX_FILE = "deepseek_html_20260402_d78591.html"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SYNC_SOURCES = [
    {
        "name": "ArcWiki template via r.jina.ai",
        "url": "https://r.jina.ai/http://arcwiki.mcd.blue/Template%3AChartConstant.json",
        "parser": "template",
    },
    {
        "name": "ArcWiki constants table via r.jina.ai",
        "url": "https://r.jina.ai/http://arcwiki.mcd.blue/index.php?mobileaction=toggle_view_desktop&title=%E5%AE%9A%E6%95%B0%E8%AF%A6%E8%A1%A8&variant=zh-tw",
        "parser": "table",
    },
]

KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_]*$", re.IGNORECASE)
CONSTANT_RE = re.compile(r"^constant\s+(\d+(?:\.\d+)?)$", re.IGNORECASE)
TABLE_LINE_RE = re.compile(r"\|")
NUM_RE = re.compile(r"\d+(?:\.\d+)?")
DIFF_ORDER = ["PST", "PRS", "FTR", "BYD", "ETR"]


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
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    charts = []
    current_key = None
    slots = []

    def flush():
        nonlocal current_key, slots
        if not current_key or not slots:
            return
        for i, v in enumerate(slots[: len(DIFF_ORDER)]):
            if v is None:
                continue
            charts.append(
                {
                    "songName": current_key,
                    "difficulty": DIFF_ORDER[i],
                    "constant": v,
                }
            )

    for line in lines:
        m = CONSTANT_RE.match(line)
        if m:
            slots.append(float(m.group(1)))
            continue

        if line.lower() == "null":
            slots.append(None)
            continue

        if line.lower().startswith("old") or line.lower() in ("true", "false"):
            continue

        if KEY_RE.match(line):
            flush()
            current_key = line.lower()
            slots = []

    flush()
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
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Arcaea-PTT-Sync/1.0",
            "Accept": "text/plain, text/html, application/json;q=0.9,*/*;q=0.8",
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
            if source["parser"] == "template":
                raw = parse_template_text(text)
            else:
                raw = parse_table_text(text)

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

    return {
        "ok": False,
        "error": "all_sources_failed",
        "details": errors,
        "charts": [],
    }


class AppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE_DIR, **kwargs)

    def do_GET(self):  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)

        if parsed.path == "/healthz":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if parsed.path in ("/", "/index.html", f"/{INDEX_FILE}"):
            self._send_index_html()
            return

        if parsed.path == "/api/songs/sync":
            payload = sync_charts_from_sources()
            self._send_json(payload, status=HTTPStatus.OK if payload.get("ok") else HTTPStatus.BAD_GATEWAY)
            return

        super().do_GET()

    def _send_index_html(self):
        index_path = os.path.join(BASE_DIR, INDEX_FILE)
        if not os.path.isfile(index_path):
            self.send_error(HTTPStatus.NOT_FOUND, f"index file not found: {INDEX_FILE}")
            return

        with open(index_path, "rb") as f:
            body = f.read()

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload, status=HTTPStatus.OK):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    port = PORT
    if len(sys.argv) >= 2:
        try:
            port = int(sys.argv[1])
        except ValueError:
            pass

    server = ThreadingHTTPServer((HOST, port), AppHandler)
    print(f"Arcaea PTT server running: http://{HOST}:{port}/{INDEX_FILE}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
