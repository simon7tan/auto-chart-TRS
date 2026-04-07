from __future__ import annotations

import csv
import re
from io import BytesIO
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

INDEX_URL = "https://www.stats.gov.cn/sj/zxfb/index.html"
BASE_URL = "https://www.stats.gov.cn/"
KEYWORD = "社会消费品零售总额"

STATE_FILE = Path("state/last_title.txt")
OUTPUT_DIR = Path("output")
OUTPUT_CSV = OUTPUT_DIR / "latest.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}


def get_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    return resp.text


def load_last_title() -> str:
    if STATE_FILE.exists():
        return STATE_FILE.read_text(encoding="utf-8").strip()
    return ""


def save_last_title(title: str) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(title.strip(), encoding="utf-8")


def normalize_first_col(value: object) -> str:
    text = "" if pd.isna(value) else str(value)

    # remove everything up to and including first colon-like separator
    text = re.sub(r"^.*?[：:]", "", text)

    # remove all whitespace, including full-width spaces and tabs/newlines
    text = re.sub(r"\s+", "", text)
    return text


def find_matching_anchor(index_html: str) -> tuple[str, str] | None:
    soup = BeautifulSoup(index_html, "lxml")

    anchors = soup.select("a.fl.pchide")
    for a in anchors:
        title_attr = (a.get("title") or "").strip()
        text = a.get_text(strip=True)
        haystack = f"{title_attr} {text}"

        if KEYWORD in haystack:
            href = a.get("href")
            if not href:
                continue
            full_href = urljoin(INDEX_URL, href)
            title = title_attr or text
            return title, full_href

    return None


def find_xls_url(article_html: str, article_url: str) -> str | None:
    soup = BeautifulSoup(article_html, "lxml")

    # prefer the exact appendix link
    for a in soup.find_all("a", href=True):
        link_title = (a.get("title") or "").strip()
        link_text = a.get_text(strip=True)
        href = a["href"]

        if ("相关数据表" in link_title) or ("相关数据表" in link_text):
            return urljoin(article_url, href)

    return None


def download_xls(xls_url: str) -> bytes:
    resp = requests.get(xls_url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.content


def extract_a17_b32_to_csv(xls_bytes: bytes, csv_path: Path) -> None:
    # Excel coordinates A17:B32 => zero-based rows 16:32 and cols 0:2
    df = pd.read_excel(BytesIO(xls_bytes), header=None, engine="xlrd")
    sliced = df.iloc[16:32, 0:2].copy()

    # clean first column
    sliced.iloc[:, 0] = sliced.iloc[:, 0].apply(normalize_first_col)

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    sliced.to_csv(csv_path, index=False, header=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)


def main() -> int:
    last_title = load_last_title()

    index_html = get_html(INDEX_URL)
    match = find_matching_anchor(index_html)

    if match is None:
        print("No matching title found. Stop.")
        return 0

    current_title, article_url = match
    print(f"Matched title: {current_title}")
    print(f"Article URL: {article_url}")

    if current_title == last_title:
        print("Matched title is same as last saved title. Stop.")
        return 0

    article_html = get_html(article_url)
    xls_url = find_xls_url(article_html, article_url)

    if not xls_url:
        raise RuntimeError("Found new title, but could not find the 相关数据表 XLS link.")

    print(f"XLS URL: {xls_url}")
    xls_bytes = download_xls(xls_url)
    extract_a17_b32_to_csv(xls_bytes, OUTPUT_CSV)
    save_last_title(current_title)

    print(f"Saved CSV to: {OUTPUT_CSV}")
    print(f"Updated last_title to: {current_title}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
