# goxplorer2.py — orevideo 専用スクレイパ
#
# ・https://orevideo.pythonanywhere.com/?sort=newest&page=N から
#   - https://video.twimg.com/...mp4?tag=xx  （twimg 生URL）
#   - https://gofile.io/d/XXXXXX             （gofile 生URL）
#   を収集
# ・gofile はページ 1〜GOFILE_PRIORITY_MAX_PAGE(デフォルト10) を優先
# ・collect_fresh_gofile_urls() で:
#   - gofile 最大 GOFILE_TARGET 本（デフォルト3本）
#   - 残りは twimg で埋めて WANT_POST 本（デフォルト5本）
# ・gofile は必ず「生存確認」してから採用
#   - HTTP ステータス
#   - HTML本文に "This content does not exist" などが出ていないか
# ・state.json（already_seen）＋このrun内で重複除外

import os
import re
import time
from typing import List, Set, Optional, Tuple

import requests

# =========================
#   基本設定
# =========================

BASE_ORIGIN = os.getenv("OREVIDEO_BASE", "https://orevideo.pythonanywhere.com").rstrip("/")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/123.0.0.0"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": BASE_ORIGIN,
    "Connection": "keep-alive",
}

RAW_LIMIT    = int(os.getenv("RAW_LIMIT", "200"))  # orevideo 用は 200 で十分
FILTER_LIMIT = int(os.getenv("FILTER_LIMIT", "80"))

# gofile を何本狙うか（1ツイート内の最大 gofile 本数）
GOFILE_TARGET = int(os.getenv("GOFILE_TARGET", "3"))

# gofile を「優先」する最大ページ（ここでは 1〜10 ページ目を優先）
GOFILE_PRIORITY_MAX_PAGE = int(os.getenv("GOFILE_PRIORITY_MAX_PAGE", "10"))

# 1run で「生存確認」を行う gofile の上限本数
MAX_GOFILE_CHECK = int(os.getenv("MAX_GOFILE_CHECK", "15"))

# twimg / gofile 抽出用
TWIMG_RE  = re.compile(r"https?://video\.twimg\.com/[^\s\"']+?\.mp4\?tag=\d+", re.I)
GOFILE_RE = re.compile(r"https?://gofile\.io/d/[A-Za-z0-9]+", re.I)


def _now() -> float:
    return time.monotonic()


def _deadline_passed(deadline_ts: Optional[float]) -> bool:
    return deadline_ts is not None and _now() >= deadline_ts


def _normalize_url(u: str) -> str:
    if not u:
        return u
    u = u.strip()
    u = re.sub(r"^http://", "https://", u, flags=re.I)
    return u.rstrip("/")


def _unique_preserve(seq: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for s in seq:
        s = s.strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


# =========================
#   HTML からリンク抽出
# =========================

def extract_links_from_html(html: str) -> Tuple[List[str], List[str]]:
    """
    orevideo のページ HTML から
      - twimg mp4
      - gofile
    を抜き出す。
    戻り値: (twimg_list, gofile_list)
    """
    if not html:
        return [], []

    tw = TWIMG_RE.findall(html)
    gf = GOFILE_RE.findall(html)

    tw_u = _unique_preserve(tw)
    gf_u = _unique_preserve(gf)

    print(f"[debug] extract_links_from_html: twimg={len(tw_u)}, gofile={len(gf_u)}")
    return tw_u, gf_u


# =========================
#   orevideo からリンク収集
# =========================

def _collect_orevideo_links(
    num_pages: int,
    deadline_ts: Optional[float],
) -> Tuple[List[str], List[str], List[str]]:
    """
    orevideo のページを 1..num_pages まで巡回してリンクを集める。
    戻り値: (twimg_all, gofile_early, gofile_late)
      - gofile_early … page <= GOFILE_PRIORITY_MAX_PAGE の gofile（優先）
      - gofile_late  … page >  GOFILE_PRIORITY_MAX_PAGE の gofile（予備）
    """
    twimg_all: List[str] = []
    gofile_early: List[str] = []
    gofile_late: List[str] = []

    total_raw = 0

    for p in range(1, num_pages + 1):
        if _deadline_passed(deadline_ts):
            print(f"[info] orevideo deadline at page={p}; stop.")
            break

        if p == 1:
            url = f"{BASE_ORIGIN}/?sort=newest&page=1"
        else:
            url = f"{BASE_ORIGIN}/?page={p}&sort=newest"

        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
        except Exception as e:
            print(f"[warn] orevideo request failed: {url} ({e})")
            continue

        if resp.status_code != 200:
            print(f"[warn] orevideo status {resp.status_code}: {url}")
            continue

        html = resp.text
        tw_list, gf_list = extract_links_from_html(html)
        print(f"[info] orevideo list {url}: twimg={len(tw_list)}, gofile={len(gf_list)}")

        twimg_all.extend(tw_list)

        if p <= GOFILE_PRIORITY_MAX_PAGE:
            gofile_early.extend(gf_list)
        else:
            gofile_late.extend(gf_list)

        total_raw = len(twimg_all) + len(gofile_early) + len(gofile_late)
        if total_raw >= RAW_LIMIT:
            print(f"[info] orevideo early stop at RAW_LIMIT={RAW_LIMIT}")
            break

        time.sleep(0.3)

    return twimg_all, gofile_early, gofile_late


# =========================
#   gofile 生存確認
# =========================

NOT_FOUND_KEYWORDS = [
    "This content does not exist",
    "The content you are looking for could not be found",
    "has been automatically removed",
    "has been deleted by the owner",
]

def _is_gofile_alive(url: str, timeout: int = 15) -> bool:
    """
    gofile のページを直接 GET して生存確認。
    - 200 以外: 基本 NG
    - HTML に NOT_FOUND_KEYWORDS が含まれていたら NG
    """
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
    except Exception as e:
        print(f"[warn] gofile(requests) failed: {url} ({e})")
        return False

    if r.status_code == 429:
        print(f"[info] gofile status 429: {url}")
        return False

    if r.status_code != 200:
        print(f"[info] gofile status {r.status_code}: {url}")
        return False

    text = (r.text or "")
    for kw in NOT_FOUND_KEYWORDS:
        if kw in text:
            print(f"[info] gofile(not found text): {url}")
            return False

    # 特に問題なければ「生きている」と判断
    print(f"[info] gofile alive: {url}")
    return True


# =========================
#   fetch_listing_pages（互換用／実際はあまり使わない）
# =========================

def fetch_listing_pages(
    num_pages: int = 100,
    deadline_ts: Optional[float] = None,
) -> List[str]:
    """
    bot.py 互換用のダミー実装。
    実際の URL 選別は collect_fresh_gofile_urls 側で行うため、
    ここでは twimg + gofile を全部まとめて返すだけ。
    """
    tw, gf_early, gf_late = _collect_orevideo_links(num_pages=num_pages, deadline_ts=deadline_ts)
    all_urls = tw + gf_early + gf_late
    return all_urls[:RAW_LIMIT]


# =========================
#   collect_fresh_gofile_urls（bot_orevideo.py から呼ばれるメイン）
# =========================

def collect_fresh_gofile_urls(
    already_seen: Set[str],
    want: int = 5,
    num_pages: int = 50,
    deadline_sec: Optional[int] = None,
) -> List[str]:
    """
    orevideo 用の URL 選別ロジック。

    - orevideo から twimg / gofile を収集
    - gofile はページ 1〜GOFILE_PRIORITY_MAX_PAGE のものを優先
    - 1ツイートあたり:
        gofile : 最大 GOFILE_TARGET 本（デフォルト 3）
        twimg  : 残りを埋めて合計 want 本（デフォルト 5）
    - gofile は必ず _is_gofile_alive() で生存確認
    - already_seen / このrun内の seen_now で重複を避ける
    - MIN_POST 未満なら [] を返す（bot_orevideo.py 側でツイートしない）
    """

    # MIN_POST を環境変数から取得（パースできなければ 1）
    try:
        min_post = int(os.getenv("MIN_POST", "1"))
    except ValueError:
        min_post = 1

    # デッドライン設定
    if deadline_sec is None:
        env = os.getenv("SCRAPE_TIMEOUT_SEC")
        try:
            if env:
                deadline_sec = int(env)
        except Exception:
            deadline_sec = None

    deadline_ts = (_now() + deadline_sec) if deadline_sec else None

    # orevideo から raw リンク収集
    tw_all_raw, gf_early_raw, gf_late_raw = _collect_orevideo_links(num_pages=num_pages, deadline_ts=deadline_ts)

    # 重複削除（ページ全体として）
    tw_all    = _unique_preserve(tw_all_raw)
    gf_early  = _unique_preserve(gf_early_raw)
    gf_late   = _unique_preserve(gf_late_raw)

    # 目標本数
    go_target = min(GOFILE_TARGET, want)
    tw_target = max(0, want - go_target)

    results: List[str] = []
    selected_gofile: List[str] = []
    selected_twimg: List[str] = []
    seen_now: Set[str] = set()

    def can_use_url(raw_url: str) -> Optional[str]:
        """state.json & この run 内での重複をチェックして OK なら正規化URLを返す"""
        if not raw_url:
            return None
        norm = _normalize_url(raw_url)
        if norm in seen_now:
            return None
        if norm in already_seen:
            return None
        return norm

    # ------- 1) gofile: 優先ページ (1〜GOFILE_PRIORITY_MAX_PAGE) -------
    gofile_checks = 0
    for url in gf_early:
        if len(selected_gofile) >= go_target:
            break
        if _deadline_passed(deadline_ts):
            print("[info] deadline reached during gofile-early selection; stop.")
            break
        if gofile_checks >= MAX_GOFILE_CHECK:
            print(f"[info] reached MAX_GOFILE_CHECK={MAX_GOFILE_CHECK}; stop gofile checks.")
            break

        norm = can_use_url(url)
        if not norm:
            continue

        gofile_checks += 1
        if _is_gofile_alive(norm):
            seen_now.add(norm)
            selected_gofile.append(norm)

    # ------- 2) gofile: それ以降のページ（足りないときだけ） -------
    if len(selected_gofile) < go_target:
        for url in gf_late:
            if len(selected_gofile) >= go_target:
                break
            if _deadline_passed(deadline_ts):
                print("[info] deadline reached during gofile-late selection; stop.")
                break
            if gofile_checks >= MAX_GOFILE_CHECK:
                print(f"[info] reached MAX_GOFILE_CHECK={MAX_GOFILE_CHECK}; stop gofile checks.")
                break

            norm = can_use_url(url)
            if not norm:
                continue

            gofile_checks += 1
            if _is_gofile_alive(norm):
                seen_now.add(norm)
                selected_gofile.append(norm)

    current_go = len(selected_gofile)
    remaining  = max(0, want - current_go)

    # ------- 3) twimg で埋める -------
    for url in tw_all:
        if len(selected_twimg) >= remaining:
            break
        if _deadline_passed(deadline_ts):
            print("[info] deadline reached during twimg selection; stop.")
            break

        norm = can_use_url(url)
        if not norm:
            continue

        seen_now.add(norm)
        selected_twimg.append(norm)

    results = selected_gofile + selected_twimg

    print(
        f"[info] orevideo selected: gofile={len(selected_gofile)}, "
        f"twimg={len(selected_twimg)}, total={len(results)} (target={want})"
    )

    # MIN_POST 未満なら「何も無かった扱い」
    if len(results) < min_post:
        print(f"[info] only {len(results)} urls collected (< MIN_POST={min_post}); return [].")
        return []

    return results[:want]
