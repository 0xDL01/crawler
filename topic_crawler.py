#!/usr/bin/env python3
# DuckDuckGo-based topic OSINT crawler (no API keys)
# Searches DDG HTML endpoint, fetches pages, filters by topic/year/country/worldwide,
# de-dupes, scores, and outputs JSON/CSV.

import re, os, csv, json, time, random, hashlib
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlencode, quote_plus, urljoin
from dateutil import parser as dateparser
import tldextract

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0 Safari/537.36"
HEADERS = {"User-Agent": UA}
DDG_HTML = "https://html.duckduckgo.com/html/"

# ---------------- Utilities ----------------

def jhash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def clean_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]):
        t.decompose()
    # drop likely noisy elements
    for t in soup.select("[aria-hidden='true'], .sr-only, .visually-hidden"):
        t.decompose()
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text

YEAR_RE = re.compile(r"\b(20\d{2})\b")

def extract_years(text: str):
    return sorted({int(y) for y in YEAR_RE.findall(text)})

def guess_published(text: str) -> Optional[str]:
    # Heuristics: look for typical date patterns and pick the newest parseable
    candidates = re.findall(
        r"\b(?:\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2}|20\d{2}-\d{1,2}-\d{1,2}|20\d{2}/\d{1,2}/\d{1,2}|[A-Za-z]{3,9}\s+\d{1,2},\s*20\d{2})\b",
        text,
    )
    dates = []
    for c in candidates:
        try:
            dt = dateparser.parse(c, dayfirst=True, fuzzy=True)
            dates.append(dt)
        except Exception:
            pass
    if dates:
        return max(dates).date().isoformat()
    return None

def find_country_hits(text: str, country: Optional[str]):
    if not country or country.lower() == "worldwide":
        return []
    c = country.lower()
    synonyms = {
        "united kingdom": ["uk", "u.k.", "britain", "british", "england", "scotland", "wales", "northern ireland"],
        "united states": ["usa", "u.s.", "america", "american", "us"],
        "uae": ["united arab emirates", "emirati", "dubai", "abu dhabi"],
        "india": ["indian", "bharat"],
        "europe": ["eu", "european union"],
    }
    keys = [c] + synonyms.get(c, [])
    tl = text.lower()
    hits = []
    for k in keys:
        if re.search(rf"\b{re.escape(k)}\b", tl):
            hits.append(k)
    return sorted(set(hits))

def summarize_lead(text: str, max_chars=450):
    sents = re.split(r"(?<=[.!?])\s+", text)
    out = []
    for s in sents:
        if len(" ".join(out)) >= max_chars:
            break
        out.append(s)
    return " ".join(out)[:max_chars].strip()

# ---------------- Search (DuckDuckGo HTML) ----------------

def ddg_search(query: str, max_results: int = 20, wait_between=1.0) -> List[Dict[str, str]]:
    """
    Uses DDG HTML endpoint (no JS) via POST.
    Returns: list of {title, url, snippet}
    """
    results = []
    # First page
    r = requests.post(DDG_HTML, headers=HEADERS, data={"q": query}, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    def parse_page(soup):
        for box in soup.select("div.result__body"):
            a = box.select_one("a.result__a")
            if not a:
                continue
            title = a.get_text(strip=True)
            href = a.get("href")
            snippet_el = box.select_one("a.result__snippet, div.result__snippet")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""
            if title and href:
                results.append({"title": title, "url": href, "snippet": snippet})

    parse_page(soup)

    # Pagination support (optional; DDG has "Next" form)
    next_form = soup.find("form", {"id": "links_form"})
    s_param = soup.find("input", {"name": "s"})
    if s_param and next_form:
        start = int(s_param.get("value", "0"))
        # Pull subsequent pages until max_results
        while len(results) < max_results:
            start += 30  # DDG increments by ~30
            payload = {"q": query, "s": str(start)}
            time.sleep(wait_between + random.random() * 0.6)
            r = requests.post(DDG_HTML, headers=HEADERS, data=payload, timeout=20)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            before = len(results)
            parse_page(soup)
            if len(results) == before:
                break  # no more new results

    return results[:max_results]

# ---------------- Fetch & Filter ----------------

def fetch_html(url: str, timeout=20) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        ctype = r.headers.get("content-type", "")
        if r.status_code == 200 and "text/html" in ctype:
            return r.text
    except Exception:
        return None
    return None

def relevance_score(text: str, topic_terms: List[str], years: Optional[List[int]], country: Optional[str]) -> float:
    t = text.lower()
    score = 0.0
    # topic keyword hits
    for term in topic_terms:
        if re.search(rf"\b{re.escape(term.lower())}\b", t):
            score += 1.0
    # year hits
    if years:
        ys = extract_years(text)
        inter = set(ys).intersection(set(years))
        score += 0.6 * len(inter)
    # country hits
    ch = find_country_hits(text, country)
    score += 0.8 * len(ch)
    # content length bonus
    if len(text) > 2000:
        score += 0.5
    return score

def crawl_topic_ddg(topic: str, year: str = "any", country: str = "worldwide", max_urls: int = 25, market_hint: str = "en-GB") -> List[Dict[str, Any]]:
    # Build query for DDG
    q_parts = [topic]
    if year and year.lower() != "any":
        q_parts.append(year)
    if country and country.lower() != "worldwide":
        q_parts.append(country)
    query = " ".join(q_parts)

    # Search
    seeds = ddg_search(query, max_results=max_urls)

    # Parse year range
    years = None
    if year and year.lower() != "any":
        m = re.match(r"^(20\d{2})(?:\s*-\s*(20\d{2}))?$", year.strip())
        if m:
            y1, y2 = int(m.group(1)), int(m.group(2) or m.group(1))
            years = list(range(y1, y2 + 1))

    topic_terms = re.findall(r"[A-Za-z0-9\-]+", topic)
    use_country = None if country.lower() == "worldwide" else country

    out = []
    seen_url = set()

    for item in seeds:
        url = item["url"]
        key = jhash(url)
        if key in seen_url:
            continue
        seen_url.add(key)

        html = fetch_html(url)
        if not html:
            continue
        text = clean_text(html)
        if len(text) < 300:
            continue

        score = relevance_score(text, topic_terms, years, use_country)
        if score < 1.0:
            continue

        years_found = extract_years(text)
        country_hits = find_country_hits(text, use_country)
        pub_date = guess_published(text)

        ext = tldextract.extract(url)
        source = ".".join([p for p in [ext.domain, ext.suffix] if p])

        rec = {
            "title": item["title"],
            "url": url,
            "source": source,
            "score": round(score, 2),
            "pub_date": pub_date,
            "years_found": years_found,
            "country_hits": country_hits,
            "snippet": item.get("snippet") or summarize_lead(text, 400),
        }
        out.append(rec)
        time.sleep(0.7 + random.random() * 0.4)  # polite delay

    # Deduplicate by normalized title
    dedup = {}
    for r in sorted(out, key=lambda x: x["score"], reverse=True):
        th = jhash((r["title"] or "").lower())
        if th not in dedup:
            dedup[th] = r
    return list(dedup.values())

# ---------------- CLI ----------------

def main():
    import argparse
    ap = argparse.ArgumentParser(description="DuckDuckGo topic OSINT crawler (no API keys).")
    ap.add_argument("--topic", required=True, help="e.g., 'cybersecurity incidents' or 'ransomware healthcare'")
    ap.add_argument("--year", default="any", help="e.g., 2025 or 2024-2025 or 'any'")
    ap.add_argument("--country", default="worldwide", help="e.g., 'United Kingdom' or 'worldwide'")
    ap.add_argument("--max", type=int, default=25, help="max URLs to fetch (default 25)")
    ap.add_argument("--out", default="results.json", help="output file (.json or .csv)")
    args = ap.parse_args()

    try:
        records = crawl_topic_ddg(args.topic, args.year, args.country, args.max)
    except requests.HTTPError as e:
        print("HTTP error:", e)
        return
    except Exception as e:
        print("Error:", e)
        return

    if not records:
        print("No records found. Try broadening your query or increasing --max.")
        return

    if args.out.lower().endswith(".csv"):
        with open(args.out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=records[0].keys())
            w.writeheader()
            w.writerows(records)
    else:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} records to {args.out}")

if __name__ == "__main__":
    main()
