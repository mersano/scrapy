import time
import csv
import re
import sys
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import tldextract
import pandas as pd

BASE = "http://pph.format.com"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PPH-Link-Extractor/1.0; +http://pph.format.com)"}

IGNORE_DOMAINS = {
    "format.com", "twitter.com", "facebook.com", "youtube.com", "linkedin.com",
    "instagram.com", "pinterest.com", "tiktok.com", "medium.com", "wordpress.com",
    "feeds.feedburner.com", "plus.google.com", "google.com", "goo.gl"
}

ARCHIVE_PATTERNS = [
    BASE + "/?page={}",
    BASE + "/blog?page={}",
    BASE + "/page/{}/",
    BASE + "/blog/page/{}/",
]

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

def get_soup(url):
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=20)
            if r.status_code == 200:
                return BeautifulSoup(r.text, "lxml")
            elif r.status_code in (403, 429, 503):
                time.sleep(2 + attempt)
            else:
                return None
        except requests.RequestException:
            time.sleep(1 + attempt)
    return None

def same_domain(url):
    try:
        return tldextract.extract(url).registered_domain == tldextract.extract(BASE).registered_domain
    except Exception:
        return False

def registered_domain(url):
    try:
        e = tldextract.extract(url)
        return ".".join([p for p in [e.domain, e.suffix] if p])
    except Exception:
        return ""

def resolve_url(href, page_url):
    if not href:
        return None
    href = href.strip()
    if href.startswith("#") or href.lower().startswith(("mailto:", "tel:")):
        return None
    return urljoin(page_url, href)

def find_post_links_on_archive(soup):
    links = set()
    for a in soup.select("a[href]"):
        href = a.get("href","")
        full = resolve_url(href, BASE)
        if not full:
            continue
        if same_domain(full):
            if re.search(r"/page/\d+/?$", full):
                continue
            if "page=" in full and re.search(r"[?&]page=\d+", full):
                continue
            if re.search(r"/(post|blog|article|news)/", full):
                links.add(full)
    return list(links)

def extract_title_and_date(soup):
    title = ""
    og = soup.select_one('meta[property="og:title"]')
    if og and og.get("content"):
        title = og["content"].strip()
    if not title and soup.title and soup.title.text:
        title = soup.title.text.strip()

    date = ""
    mt = soup.select_one('meta[property="article:published_time"]')
    if mt and mt.get("content"):
        date = mt["content"].strip()
    if not date:
        t = soup.find("time")
        if t and (t.get("datetime") or t.text):
            date = (t.get("datetime") or t.text).strip()
    return title, date

def extract_external_links_from_post(post_url):
    soup = get_soup(post_url)
    if not soup:
        return [], ("","")
    title, date = extract_title_and_date(soup)

    containers = soup.select("article, main, .post, .entry-content") or [soup]
    rows, seen = [], set()
    for c in containers:
        for a in c.select("a[href]"):
            href = resolve_url(a.get("href"), post_url)
            if not href or same_domain(href):
                continue
            dom = registered_domain(href)
            if not dom or dom in IGNORE_DOMAINS:
                continue
            anchor = " ".join(a.get_text(" ", strip=True).split())
            rel = a.get("rel") or []
            rel = " ".join(rel) if isinstance(rel, list) else str(rel)
            nofollow = "nofollow" in rel.lower()
            key = (href, anchor)
            if key in seen:
                continue
            seen.add(key)
            rows.append({
                "article_title": title,
                "article_url": post_url,
                "article_date": date,
                "link_url": href,
                "link_anchor": anchor,
                "link_rel": rel,
                "link_nofollow": nofollow,
                "link_domain": dom
            })
    return rows, (title, date)

def discover_archive_pages(max_pages=150):
    discovered, chosen_pattern = set(), None
    soup0 = get_soup(BASE)
    if soup0:
        for p in find_post_links_on_archive(soup0):
            discovered.add(p)

    for pattern in ARCHIVE_PATTERNS:
        empty_streak = 0
        for n in range(1, max_pages + 1):
            url = pattern.format(n)
            soup = get_soup(url)
            if not soup:
                empty_streak += 1
                if empty_streak >= 3: break
                continue
            posts = find_post_links_on_archive(soup)
            if posts:
                chosen_pattern = pattern
                empty_streak = 0
                for p in posts:
                    discovered.add(p)
            else:
                empty_streak += 1
                if empty_streak >= 3: break
        if chosen_pattern:
            break
    return list(discovered)

def main():
    print("Descopăr articolele din arhivă…")
    post_urls = discover_archive_pages()
    if not post_urls:
        print("Nu am găsit linkuri către articole. Putem ajusta scriptul dacă îmi dai un exemplu de URL de articol.")
        sys.exit(0)

    print(f"Am găsit {len(post_urls)} posibile articole. Încep extragerea…")
    all_rows = []
    for i, u in enumerate(sorted(set(post_urls))):
        print(f"[{i+1}/{len(post_urls)}] {u}")
        rows, _ = extract_external_links_from_post(u)
        all_rows.extend(rows)
        time.sleep(0.5)

    if not all_rows:
        print("Nu am găsit linkuri externe în articole.")
        sys.exit(0)

    detail_csv = "clients_links.csv"
    with open(detail_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "article_title","article_url","article_date",
            "link_url","link_anchor","link_rel","link_nofollow","link_domain"
        ])
        writer.writeheader()
        writer.writerows(all_rows)
    print(f"Scris: {detail_csv} ({len(all_rows)} rânduri)")

    df = pd.DataFrame(all_rows)
    df_domains = (df.sort_values(["link_domain","article_date"], ascending=[True, False])
                    .drop_duplicates(subset=["link_domain"])[
                        ["link_domain","link_url","article_title","article_url","article_date","link_anchor"]
                    ])
    dom_csv = "clients_domains_unique.csv"
    df_domains.to_csv(dom_csv, index=False, encoding="utf-8")
    print(f"Scris: {dom_csv} ({len(df_domains)} domenii unice)")

if __name__ == "__main__":
    main()
