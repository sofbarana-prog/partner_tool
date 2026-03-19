import json
import re
import time
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright

OUT_JSON = Path("calls.json")

BASE = "https://ec.europa.eu"
LIST_URL = (
    "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals"
    "?order=DESC&pageNumber={page}&pageSize=50&sortBy=startDate"
    "&isExactMatch=true&status=31094501,31094502&programmePeriod=2021%20-%202027"
)

MAX_PAGES = 40
HEADLESS = True
SLEEP_BETWEEN_DETAIL = 0.25

CLUSTER_MAP = {
    "1": "Health",
    "2": "Culture, Creativity and Inclusive Society",
    "3": "Civil Security for Society",
    "4": "Digital, Industry and Space",
    "5": "Climate, Energy and Mobility",
    "6": "Food, Bioeconomy, Natural Resources, Agriculture and Environment",
}

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}

def norm(v):
    if v is None:
        return ""
    return re.sub(r"\s+", " ", str(v)).strip()

def parse_date_to_iso(value):
    s = norm(value)
    if not s:
        return ""

    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.search(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b", s)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year = int(m.group(3))
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return ""

    m = re.search(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b", s)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = MONTHS.get(month_name)
        if month:
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                return ""

    return ""

def normalize_programme(v):
    s = norm(v)
    sl = s.lower()
    if not s:
        return ""
    if sl == "eu programmes":
        return "EU External Action-Prospect"
    if "horizon" in sl:
        return "Horizon Europe"
    if "digital europe" in sl or sl == "digital":
        return "Digital Europe"
    if "creative europe" in sl or "crea" in sl:
        return "Creative Europe (CREA)"
    if "agrip" in sl:
        return "AGRIP"
    if "defence fund" in sl or sl == "edf" or "european defence fund" in sl:
        return "EDF"
    if "single market" in sl or "smp" in sl:
        return "Single Market Programme"
    if "external action" in sl or "prospect" in sl:
        return "EU External Action-Prospect"
    return s

def normalize_action(v):
    s = norm(v)
    sl = s.lower()
    if not s:
        return ""
    if "research and innovation action" in sl:
        return "RIA"
    if "innovation action" in sl:
        return "IA"
    if "coordination and support action" in sl:
        return "CSA"
    if "cofund" in sl:
        return "COFUND"
    return s

def extract_cluster_num(*values):
    for v in values:
        s = norm(v)
        if not s:
            continue
        if s in {"1", "2", "3", "4", "5", "6"}:
            return s

        m = re.search(r"HORIZON-CL([1-6])", s, re.IGNORECASE)
        if m:
            return m.group(1)

        m = re.search(r"Cluster\s*([1-6])", s, re.IGNORECASE)
        if m:
            return m.group(1)
    return ""

def infer_thematic_cluster(cluster_num, programme, programme_raw, action, record_type, name, url):
    cluster_num = norm(cluster_num)
    programme = norm(programme)
    programme_raw = norm(programme_raw)
    action = norm(action)
    record_type = norm(record_type)
    name = norm(name)
    url = norm(url)

    p = programme.lower()
    pr = programme_raw.lower()
    text = " ".join([programme, programme_raw, action, record_type, name, url]).lower()

    if cluster_num == "1":
        return "Health & Life Sciences", "official_cluster_1"
    if cluster_num == "2":
        return "Culture, Creativity & Inclusion", "official_cluster_2"
    if cluster_num == "3":
        return "Security & Resilience", "official_cluster_3"
    if cluster_num == "4":
        return "Digital, Industry & Space", "official_cluster_4"
    if cluster_num == "5":
        return "Climate, Energy & Mobility", "official_cluster_5"
    if cluster_num == "6":
        return "Food, Bioeconomy & Environment", "official_cluster_6"

    if "digital europe" in p or "digital europe" in pr:
        return "Digital, Industry & Space", "programme_digital_europe"
    if "creative europe" in p or "creative europe" in pr or "crea" in p or "crea" in pr:
        return "Culture, Creativity & Inclusion", "programme_crea"
    if "agrip" in p or "agrip" in pr:
        return "Food, Bioeconomy & Environment", "programme_agrip"
    if "edf" in p or "edf" in pr or "european defence fund" in p or "european defence fund" in pr:
        return "Defence", "programme_edf"
    if "single market programme" in p or "single market programme" in pr or p == "smp" or pr == "smp":
        return "SME, Entrepreneurship & Market Uptake", "programme_smp"
    if "external action" in p or "external action" in pr or "prospect" in p or "prospect" in pr:
        return "External Action & International Cooperation", "programme_external_action"

    if "competitive" in record_type.lower():
        if any(k in text for k in ["digital", "ai", "data", "cloud", "chip", "cyber", "software"]):
            return "Digital, Industry & Space", "competitive_keyword_digital"
        if any(k in text for k in ["food", "agri", "farm", "agriculture", "bio", "environment"]):
            return "Food, Bioeconomy & Environment", "competitive_keyword_agri"
        return "SME, Entrepreneurship & Market Uptake", "competitive_default"

    if any(k in text for k in ["health", "cancer", "medical", "clinical", "hospital", "biotech", "pharma"]):
        return "Health & Life Sciences", "keyword_health"
    if any(k in text for k in ["culture", "creative", "heritage", "democracy", "inclusive", "inclusion", "society"]):
        return "Culture, Creativity & Inclusion", "keyword_culture"
    if any(k in text for k in ["security", "cybersecurity", "cyber", "crime", "border", "resilience", "civil protection"]):
        return "Security & Resilience", "keyword_security"
    if any(k in text for k in ["digital", "ai", "artificial intelligence", "data", "cloud", "semiconductor", "quantum", "space", "robotics", "chip", "software"]):
        return "Digital, Industry & Space", "keyword_digital"
    if any(k in text for k in ["climate", "energy", "mobility", "transport", "battery", "hydrogen", "decarbon", "emission", "renewable"]):
        return "Climate, Energy & Mobility", "keyword_climate_energy"
    if any(k in text for k in ["food", "agri", "agriculture", "farm", "bioeconomy", "biodiversity", "environment", "nature", "soil", "water"]):
        return "Food, Bioeconomy & Environment", "keyword_food_environment"
    if any(k in text for k in ["sme", "startup", "entrepreneur", "business", "market uptake", "scale-up", "cascade", "fstp"]):
        return "SME, Entrepreneurship & Market Uptake", "keyword_sme"
    if any(k in text for k in ["international cooperation", "development", "humanitarian", "neighbourhood", "partner countries", "civil society"]):
        return "External Action & International Cooperation", "keyword_external_action"

    return "Cross-cutting / Other", "fallback"

def beneficiary_hint(record_type, action, programme, thematic_cluster):
    rt = norm(record_type).lower()
    a = norm(action).lower()
    p = norm(programme).lower()
    tc = norm(thematic_cluster).lower()

    hints = []

    if "competitive" in rt:
        hints.append("SME")

    if a == "ia":
        hints.extend(["SME", "Large enterprise", "Research organisation"])
    elif a == "ria":
        hints.extend(["Research organisation", "SME", "Large enterprise"])
    elif a == "csa":
        hints.extend(["Research organisation", "Public body", "NGO", "SME"])

    if "external action" in p or "external action" in tc:
        hints.extend(["NGO", "Public body", "Research organisation"])

    if "defence" in tc:
        hints.extend(["Large enterprise", "Research organisation", "SME"])

    out = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out

def accept_cookies(page):
    labels = [
        "Accept all", "Accept All", "Accept", "I accept",
        "Accetta", "Accetta tutto", "Accept cookies"
    ]
    for label in labels:
        try:
            btn = page.get_by_role("button", name=re.compile(label, re.IGNORECASE))
            if btn.count():
                btn.first.click(timeout=1500)
                page.wait_for_timeout(700)
                return
        except Exception:
            pass

def collect_links(page):
    hrefs = page.eval_on_selector_all(
        'a[href*="/topic-details/"], a[href*="/competitive-calls-cs/"], a[href*="/prospect-details/"]',
        "els => els.map(e => e.href)"
    )
    seen = set()
    out = []
    for h in hrefs:
        if h and h not in seen:
            seen.add(h)
            out.append(h)
    return out

def read_text_near_label(text, label):
    if not text:
        return ""
    patterns = [
        rf"{re.escape(label)}\s*:\s*([^\n\r]+)",
        rf"{re.escape(label)}\s*\n([^\n\r]+)",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return norm(m.group(1))
    return ""

def scrape_detail(page, url):
    page.goto(url, wait_until="networkidle", timeout=60000)
    page.wait_for_timeout(1200)
    accept_cookies(page)

    text = ""
    try:
        text = page.locator("body").inner_text()
    except Exception:
        pass

    title = ""
    try:
        h1 = page.locator("h1").first
        if h1.count():
            title = norm(h1.inner_text())
    except Exception:
        pass

    programme_raw = (
        read_text_near_label(text, "Programme")
        or read_text_near_label(text, "Programme:")
    )
    programme = normalize_programme(programme_raw)

    action_raw = (
        read_text_near_label(text, "Type of action")
        or read_text_near_label(text, "Type of Action")
    )
    action = normalize_action(action_raw)

    opening = (
        read_text_near_label(text, "Opening date")
        or read_text_near_label(text, "Planned opening date")
    )
    deadline = (
        read_text_near_label(text, "Deadline date")
        or read_text_near_label(text, "Deadline")
        or read_text_near_label(text, "Next deadline")
    )

    budget = (
        read_text_near_label(text, "Total funding available")
        or ""
    )

    # budget overview fallback
    if not budget:
        m = re.search(r"(€\s?[\d\.,]+|[\d\.,]+\s?€)", text)
        if m:
            budget = norm(m.group(1))

    record_type = ""
    if "/topic-details/" in url:
        record_type = "topic-details"
    elif "/competitive-calls-cs/" in url:
        record_type = "competitive-calls"
    elif "/prospect-details/" in url:
        record_type = "prospect-details"

    cluster_num = extract_cluster_num(title, programme_raw, url)
    cluster_label = CLUSTER_MAP.get(cluster_num, "")

    thematic_cluster, thematic_reason = infer_thematic_cluster(
        cluster_num=cluster_num,
        programme=programme,
        programme_raw=programme_raw,
        action=action,
        record_type=record_type,
        name=title,
        url=url
    )

    return {
        "name": title,
        "programme": programme,
        "programme_raw": programme_raw,
        "cluster_num": cluster_num,
        "cluster_label": cluster_label,
        "thematic_cluster": thematic_cluster,
        "thematic_cluster_reason": thematic_reason,
        "action": action,
        "action_raw": action_raw,
        "opening": opening,
        "opening_iso": parse_date_to_iso(opening),
        "deadline": deadline,
        "deadline_iso": parse_date_to_iso(deadline),
        "budget": budget,
        "url": url,
        "record_type": record_type,
        "beneficiary_hint": beneficiary_hint(
            record_type=record_type,
            action=action,
            programme=programme,
            thematic_cluster=thematic_cluster
        ),
    }

def main():
    calls = []
    seen = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        ctx = browser.new_context(
            viewport={"width": 1600, "height": 1200},
            locale="en-US",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        all_links = []
        for page_num in range(1, MAX_PAGES + 1):
            url = LIST_URL.format(page=page_num)
            print(f"[LIST] page {page_num}")
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                page.wait_for_timeout(1500)
                accept_cookies(page)
                links = collect_links(page)
                print(f"  links found: {len(links)}")
                if not links:
                    break
                new_links = [x for x in links if x not in seen]
                if not new_links:
                    break
                for x in new_links:
                    seen.add(x)
                    all_links.append(x)
            except Exception as e:
                print(f"  page failed: {e}")
                break

        print(f"\nTotal unique links collected: {len(all_links)}")

        detail_page = ctx.new_page()
        for i, link in enumerate(all_links, 1):
            print(f"[DETAIL {i}/{len(all_links)}] {link}")
            try:
                row = scrape_detail(detail_page, link)
                if row["url"]:
                    calls.append(row)
            except Exception as e:
                print(f"  detail failed: {e}")
            time.sleep(SLEEP_BETWEEN_DETAIL)

        browser.close()

    # deduplicate again by url
    uniq = {}
    for c in calls:
        uniq[c["url"]] = c

    payload = {"calls": list(uniq.values())}
    OUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"\nCreated {OUT_JSON.resolve()} with {len(payload['calls'])} calls")

if __name__ == "__main__":
    main()
