# bot_orevideo.py — orevideo 用（ロジックは bot.py と同じ、goxplorer2 を使うだけ）

import json, os, re, time
from datetime import datetime, timezone, timedelta
from dateutil import tz
import tweepy
from playwright.sync_api import sync_playwright

from goxplorer2 import collect_fresh_gofile_urls  # ← ここだけ違う

import requests
try:
    from requests_oauthlib import OAuth1
except ImportError:
    OAuth1 = None

# 追加: Google Sheets 用
import gspread
from google.oauth2.service_account import Credentials

AFFILIATE_URL = "https://www.effectivegatecpm.com/ra1dctjqd?key=7386f2c3cdf8ea912bbf6b2ab000fd44"
STATE_FILE = "state.json"
DAILY_LIMIT = 16
JST = tz.gettz("Asia/Tokyo")
TWEET_LIMIT = 280
TCO_URL_LEN = 23
GOFILE_RE = re.compile(r"https?://gofile\.io/d/[A-Za-z0-9]+", re.I)

ZWSP = "\u200B"; ZWNJ = "\u200C"; INVISIBLES = [ZWSP, ZWNJ]

def _env_int(key, default):
    try: return int(os.getenv(key, str(default)))
    except: return default

WANT_POST = _env_int("WANT_POST", 5)   # ← YMLから渡す
MIN_POST  = _env_int("MIN_POST", 3)
HARD_LIMIT_SEC = _env_int("HARD_LIMIT_SEC", 600)
USE_API_TIMELINE = _env_int("USE_API_TIMELINE", 0)

def _default_state():
    return {"posted_urls": [], "last_post_date": None, "posts_today": 0,
            "recent_urls_24h": [], "line_seq": 1}

def load_state():
    if not os.path.exists(STATE_FILE): return _default_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = _default_state()
    for k, v in _default_state().items():
        if k not in data: data[k] = v
    return data

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def reset_if_new_day(state, now_jst):
    today = now_jst.date().isoformat()
    if state.get("last_post_date") != today:
        state["last_post_date"] = today
        state["posts_today"] = 0

def purge_recent_12h(state, now_utc):
    cutoff = now_utc - timedelta(hours=12)
    buf = []
    for item in state.get("recent_urls_24h", []):
        try:
            ts = datetime.fromisoformat(item.get("ts"))
            if ts >= cutoff: buf.append(item)
        except: pass
    state["recent_urls_24h"] = buf

def normalize_url(u):
    if not u: return u
    u = u.strip()
    u = re.sub(r"^http://", "https://", u, flags=re.I)
    return u.rstrip("/")

def build_seen_set_from_state(state):
    seen = set()
    for u in state.get("posted_urls", []): seen.add(normalize_url(u))
    for it in state.get("recent_urls_24h", []): seen.add(normalize_url(it.get("url")))
    return seen

def estimate_tweet_len_tco(text: str) -> int:
    def repl(m): return "U" * TCO_URL_LEN
    return len(re.sub(r"https?://\S+", repl, text))

def compose_fixed5_text(gofile_urls, start_seq: int, salt_idx: int = 0, add_sig: bool = True):
    invis = INVISIBLES[salt_idx % len(INVISIBLES)]
    lines, seq = [], start_seq
    take = min(WANT_POST, len(gofile_urls))
    for i, u in enumerate(gofile_urls[:take]):
        lines.append(f"{seq}{invis}. {u}")
        if i < take - 1: lines.append(AFFILIATE_URL)
        seq += 1
    text = "\n".join(lines)
    if add_sig:
        seed = (start_seq * 1315423911) ^ int(time.time() // 60)
        sig = "".join(INVISIBLES[(seed >> i) & 1] for i in range(16))
        text += sig
    return text, take

def get_client():
    return tweepy.Client(
        bearer_token=None,
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
        wait_on_rate_limit=bool(_env_int("WAIT_ON_RATE_LIMIT", 0)),
    )

def fetch_recent_urls_via_web(username: str, scrolls: int = 1, wait_ms: int = 800) -> set:
    # 既定では使わない（USE_API_TIMELINE=1の場合などで使う）
    if not username: return set()
    url = f"https://x.com/{username}"
    seen = set()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/123.0.0.0"),
            locale="ja-JP")
        page = ctx.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(wait_ms)
        for _ in range(scrolls):
            page.mouse.wheel(0, 1800); page.wait_for_timeout(wait_ms)
        html = page.content()
        ctx.close(); browser.close()
    for m in GOFILE_RE.findall(html):
        seen.add(normalize_url(m))
    return seen

def post_to_x_v2(client, text, quote_tweet_id=None):
    if quote_tweet_id:
        return client.create_tweet(text=text, quote_tweet_id=quote_tweet_id)
    return client.create_tweet(text=text)

def _oauth1_session():
    if OAuth1 is None:
        raise RuntimeError("requests-oauthlib が必要です。requirements.txt に 'requests-oauthlib==1.3.1' を追加してください。")
    return OAuth1(
        os.environ["X_API_KEY"],
        os.environ["X_API_SECRET"],
        os.environ["X_ACCESS_TOKEN"],
        os.environ["X_ACCESS_TOKEN_SECRET"],
        signature_type='auth_header'
    )

def post_to_community_via_undocumented_api(status_text: str, community_id: str):
    # 旧Twitterエンドポイント。環境によっては https://api.x.com/2/tweets でも可
    url = "https://api.twitter.com/2/tweets"
    payload = {"text": status_text, "community_id": str(community_id)}
    sess = _oauth1_session()
    headers = {"Content-Type": "application/json"}
    r = requests.post(url, headers=headers, data=json.dumps(payload), auth=sess, timeout=30)
    try:
        body = r.json()
    except Exception:
        body = r.text
    if not r.ok:
        raise RuntimeError(f"community post failed {r.status_code}: {body}")
    return body

# =========================
#   Google Sheets 関連（B列URL優先 & E列にポスト済み）
# =========================

_GSHEET_WS = None

def _get_worksheet():
    """
    環境変数:
      - GSPREAD_SERVICE_ACCOUNT_JSON: サービスアカウントのJSON文字列
      - OREVIDEO_SHEET_URL: スプレッドシートのURL
      - OREVIDEO_SHEET_NAME: ワークシート名（省略時は1枚目）
    """
    global _GSHEET_WS
    if _GSHEET_WS is not None:
        return _GSHEET_WS

    sa_json = os.getenv("GSPREAD_SERVICE_ACCOUNT_JSON")
    sheet_url = os.getenv("OREVIDEO_SHEET_URL")

    if not sa_json or not sheet_url:
        print("[info] Google Sheets env not set; skip sheet usage.")
        return None

    try:
        info = json.loads(sa_json)
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(sheet_url)
        ws_name = os.getenv("OREVIDEO_SHEET_NAME", "").strip()
        if ws_name:
            ws = sh.worksheet(ws_name)
        else:
            ws = sh.sheet1
        _GSHEET_WS = ws
        print(f"[info] Google Sheets connected (worksheet={ws.title})")
        return ws
    except Exception as e:
        print(f"[warn] Google Sheets init failed: {e}")
        return None

def fetch_sheet_urls(want: int):
    """
    スプレッドシートから候補URLを取得する。

    仕様:
      - B列にURL
      - E列が空の行のみ未投稿扱い
      - 1行目はヘッダーとみなして 2行目以降を見る
      - 上から want 件まで取得
    戻り値:
      [ (url, row_index), ... ]
    """
    ws = _get_worksheet()
    if not ws:
        return []

    try:
        values = ws.get_all_values()
    except Exception as e:
        print(f"[warn] Google Sheets get_all_values failed: {e}")
        return []

    entries = []
    for row_idx, row in enumerate(values, start=1):
        if row_idx == 1:
            # 1行目はヘッダー想定
            continue

        b = ""
        e_val = ""
        if len(row) > 1 and row[1]:
            b = row[1].strip()
        if len(row) > 4 and row[4]:
            e_val = row[4].strip()

        if not b:
            continue
        if e_val:
            # E列に何か書いてあれば「ポスト済み」とみなしてスキップ
            continue

        url = normalize_url(b)
        entries.append((url, row_idx))
        if len(entries) >= want:
            break

    print(f"[info] sheet candidate urls: {len(entries)}")
    return entries

def mark_sheet_posted(row_indices):
    """
    投稿に使ったスプレッドシート行の E列 に「ポスト済み」を書き込む。
    """
    if not row_indices:
        return
    ws = _get_worksheet()
    if not ws:
        return

    for r in row_indices:
        try:
            ws.update_cell(r, 5, "ポスト済み")
        except Exception as e:
            print(f"[warn] Google Sheets update_cell failed (row={r}): {e}")

# =========================
#   main ロジック
# =========================

def main():
    start_ts = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    now_jst = now_utc.astimezone(JST)

    state = load_state()
    purge_recent_12h(state, now_utc)
    reset_if_new_day(state, now_jst)

    if state.get("posts_today", 0) >= DAILY_LIMIT:
        print("Daily limit reached; skip."); return

    already_seen = build_seen_set_from_state(state)

    # 既出チェックは state を主に使用。必要時だけTLを見る（既定OFF）。
    if USE_API_TIMELINE:
        try:
            client_tmp = get_client()
            me = client_tmp.get_me(user_auth=True)
            user = me.data if me and me.data else None
            username = getattr(user, "username", None)
        except Exception:
            username = os.getenv("X_SCREEN_NAME", None)
        web_seen = fetch_recent_urls_via_web(username, scrolls=1, wait_ms=800) if username else set()
        if web_seen:
            already_seen |= web_seen
        print(f"[info] recent timeline gofiles via WEB (opt): {len(web_seen)} (user={username})")
    else:
        print("[info] timeline check skipped (USE_API_TIMELINE=0)")

    if (time.monotonic() - start_ts) > HARD_LIMIT_SEC:
        print("[warn] time budget exceeded before collection; abort."); return

    # 1) まずは Google スプレッドシートから URL を優先的に取得
    sheet_entries = fetch_sheet_urls(WANT_POST)
    sheet_urls = [u for (u, _row) in sheet_entries]
    sheet_rows = [_row for (_u, _row) in sheet_entries]

    remaining_want = max(0, WANT_POST - len(sheet_urls))
    scraped_urls = []

    # 2) シートだけで足りない分だけ orevideo スクレイピングで穴埋め
    if remaining_want > 0:
        # 締切
        try:
            deadline_env = os.getenv("SCRAPE_TIMEOUT_SEC")
            deadline_sec = int(deadline_env) if deadline_env else None
        except Exception:
            deadline_sec = None

        urls_from_scrape = collect_fresh_gofile_urls(
            already_seen=already_seen,
            want=remaining_want,
            num_pages=int(os.getenv("NUM_PAGES", "50")),
            deadline_sec=deadline_sec
        )
        scraped_urls = urls_from_scrape or []
        print(f"[info] collected alive urls from orevideo: {len(scraped_urls)}")
    else:
        print("[info] sheet urls enough; orevideo scraping skipped.")

    # 3) シート由来 + スクレイピング由来を合算
    all_urls = sheet_urls + scraped_urls
    print(f"[info] total candidate urls (sheet+orevideo): {len(all_urls)}")

    if len(all_urls) < MIN_POST:
        print("Not enough URLs; skip.")
        return

    # そのまま投稿（ロジックは既存どおり）
    start_seq = int(state.get("line_seq", 1))
    salt = (now_jst.hour + now_jst.minute) % len(INVISIBLES)
    status_text, taken = compose_fixed5_text(all_urls, start_seq=start_seq, salt_idx=salt, add_sig=True)

    if estimate_tweet_len_tco(status_text) > TWEET_LIMIT:
        status_text = status_text.replace(". https://", ".https://")
    while estimate_tweet_len_tco(status_text) > TWEET_LIMIT:
        status_text = status_text.rstrip(ZWSP + ZWNJ)

    community_id = os.getenv("X_COMMUNITY_ID", "").strip()
    client = get_client()

    tweet_id = None

    if community_id:
        # 1) コミュニティに投稿（失敗しても落ちずに通常ツイートへフォールバック）
        comm_id = None
        try:
            resp_comm = post_to_community_via_undocumented_api(status_text, community_id)
            comm_id = resp_comm.get("data", {}).get("id") if isinstance(resp_comm, dict) else None
            print(f"[info] community posted id={comm_id}")
        except Exception as e:
            print(f"[warn] community post failed; fallback to normal tweet: {e}")

        # 2) コミュニティ投稿が成功して ID が取れたら引用ツイート、失敗時は通常ポスト
        if comm_id:
            resp = post_to_x_v2(client, status_text, quote_tweet_id=comm_id)
            tweet_id = resp.data.get("id") if resp and resp.data else None
            print(f"[info] tweeted id={tweet_id} (quote community)")
        else:
            resp = post_to_x_v2(client, status_text)
            tweet_id = resp.data.get("id") if resp and resp.data else None
            print(f"[info] tweeted id={tweet_id} (fallback normal)")
    else:
        # 通常ポストのみ
        resp = post_to_x_v2(client, status_text)
        tweet_id = resp.data.get("id") if resp and resp.data else None
        print(f"[info] tweeted id={tweet_id}")

    # 4) state 更新（投稿に使った URL を記録）
    used_count = min(taken, len(all_urls))
    used_urls = all_urls[:used_count]

    for u in used_urls:
        if u not in state["posted_urls"]:
            state["posted_urls"].append(u)
        state["recent_urls_24h"].append({"url": u, "ts": now_utc.isoformat()})

    state["posts_today"] = state.get("posts_today", 0) + 1
    state["line_seq"] = start_seq + used_count
    save_state(state)

    # 5) シート由来の URL について E列を「ポスト済み」に更新
    if sheet_rows:
        used_sheet_rows = sheet_rows[:min(used_count, len(sheet_rows))]
        mark_sheet_posted(used_sheet_rows)

    print(f"Posted ({used_count} urls):", status_text)

if __name__ == "__main__":
    main()
