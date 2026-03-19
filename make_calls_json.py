import json
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

IN_XLSX = Path("calls_horizon_export.xlsx")
OUT_JSON = Path("calls.json")

CLUSTER_MAP = {
    "1": "Health",
    "2": "Culture, Creativity and Inclusive Society",
    "3": "Civil Security for Society",
    "4": "Digital, Industry and Space",
    "5": "Climate, Energy and Mobility",
    "6": "Food, Bioeconomy, Natural Resources, Agriculture and Environment",
    "M-CIT":   "Climate-neutral & Smart Cities",
    "M-OCEAN": "Healthy Oceans, Seas, Coastal & Inland Waters",
}

THEMATIC_MAP = {
    "1":       "Health & Life Sciences",
    "2":       "Culture, Creativity & Inclusion",
    "3":       "Security & Resilience",
    "4":       "Digital, Industry & Space",
    "5":       "Climate, Energy & Mobility",
    "6":       "Food, Bioeconomy & Environment",
    "M-CIT":   "Climate-neutral & Smart Cities",
    "M-OCEAN": "Healthy Oceans, Seas, Coastal & Inland Waters",
}

PROGRAMME_THEMATIC_MAP = {
    "European Defence Fund":           "Defence",
    "EDF":                             "Defence",
    "EU External Action":              "External Action & International Cooperation",
    "EU External Action-Prospect":     "External Action & International Cooperation",
    "Single Market Programme (SMP)":   "SME, Entrepreneurship & Market Uptake",
    "Single Market Programme":         "SME, Entrepreneurship & Market Uptake",
    "CERV":                            "Culture, Creativity & Inclusion",
    "Creative Europe (CREA)":          "Culture, Creativity & Inclusion",
    "Erasmus+":                        "Culture, Creativity & Inclusion",
    "European Social Fund+ (ESF+)":    "Culture, Creativity & Inclusion",
    "Just Transition Mechanism (JTM)": "Climate, Energy & Mobility",
    "Innovation Fund (INNOVFUND)":     "Climate, Energy & Mobility",
    "EMFAF":                           "Food, Bioeconomy & Environment",
    "LIFE":                            "Food, Bioeconomy & Environment",
    "Euratom":                         "Climate, Energy & Mobility",
    "Connecting Europe Facility (CEF)":"Climate, Energy & Mobility",
    "Internal Security Fund (ISF)":    "Security & Resilience",
    "European Solidarity Corps (ESC)": "Culture, Creativity & Inclusion",
    "Digital Europe Programme":        "Digital, Industry & Space",
    # Additional programmes found in unclassified calls
    "RENEWFM":                         "Climate, Energy & Mobility",
    "SOCPL":                           "Culture, Creativity & Inclusion",
    "JUST":                            "Culture, Creativity & Inclusion",
    "Pericles IV":                     "Culture, Creativity & Inclusion",
    "I3":                              "SME, Entrepreneurship & Market Uptake",
    # ERC → cross-cutting (Horizon Europe Pillar 1, all fields)
    "ERC":                             "Cross-cutting / Other",
    # EMFAF numeric ID fallback
    "43392145":                        "Food, Bioeconomy & Environment",
    # Generic fallback for Horizon Europe / Digital Europe calls with no cluster_num
    # (competitive-calls-cs entries where call_id was not yet scraped)
    "Horizon Europe":                  "Cross-cutting / Other",
    "Digital Europe":                  "Digital, Industry & Space",
}

# URL keyword rules: (prefix, subcode_or_None, cluster_num, cluster_label, thematic)
# - prefix    : must appear anywhere in the topic ID (uppercase)
# - subcode   : if not None, must also appear as -SUBCODE- segment (for MISS- patterns)
# - cluster_num / cluster_label : override cluster fields ("" = keep original)
# - thematic  : override thematic_cluster field ("" = keep original)
# Rules are evaluated in order; first match wins.
URL_RULES = [
    # Horizon Missions (subcode required)
    ("MISS", "CIT",    "M-CIT",  "Climate-neutral & Smart Cities",               "Climate-neutral & Smart Cities"),
    ("MISS", "OCEAN",  "M-OCEAN","Healthy Oceans, Seas, Coastal & Inland Waters", "Healthy Oceans, Seas, Coastal & Inland Waters"),
    ("MISS", "CLIMA",  "5",      "Climate, Energy and Mobility",                  "Climate, Energy & Mobility"),
    ("MISS", "CANCER", "1",      "Health",                                        "Health & Life Sciences"),
    ("MISS", "SOIL",   "6",      "Food, Bioeconomy, Natural Resources, Agriculture and Environment", "Food, Bioeconomy & Environment"),

    # Horizon health cluster
    ("HLTH",            None, "1",  "Health",                     "Health & Life Sciences"),

    # EIC / EIE / EIT → SME & market uptake
    ("EIC",             None, "",   "",                            "SME, Entrepreneurship & Market Uptake"),
    ("EIE",             None, "",   "",                            "SME, Entrepreneurship & Market Uptake"),
    ("EIT",             None, "",   "",                            "SME, Entrepreneurship & Market Uptake"),

    # CID → climate
    ("CID",             None, "5",  "Climate, Energy and Mobility","Climate, Energy & Mobility"),

    # EURATOM → climate/energy
    ("EURATOM",         None, "5",  "Climate, Energy and Mobility","Climate, Energy & Mobility"),

    # EUROHPC → digital
    ("EUROHPC",         None, "4",  "Digital, Industry and Space", "Digital, Industry & Space"),

    # JU Clean Aviation → new thematic area
    ("JU-CLEAN-AVIATION", None, "", "",                            "Clean Aviation"),
    # Other JU (catch-all after specific JU rules above)
    ("JU-",             None, "",   "",                            "Climate, Energy & Mobility"),

    # MSCA → cross-cutting, research orgs only (beneficiary override handled separately)
    ("MSCA",            None, "",   "",                            "Cross-cutting / Other"),

    # NEB (New European Bauhaus) → Climate-neutral & Smart Cities
    ("NEB",             None, "",   "",                            "Climate-neutral & Smart Cities"),

    # RAISE → cross-cutting
    ("RAISE",           None, "",   "",                            "Cross-cutting / Other"),

    # WIDERA → cross-cutting
    ("WIDERA",          None, "",   "",                            "Cross-cutting / Other"),

    # ── New rules ────────────────────────────────────────────────────────────
    ("INFRA",           None, "",   "",                            "Cross-cutting / Other"),
    ("AGRIP",           None, "6",  "Food, Bioeconomy, Natural Resources, Agriculture and Environment",
                                                                   "Food, Bioeconomy & Environment"),
    ("EUAF",            None, "",   "",                            "Cross-cutting / Other"),
    # HORIZON-MISS-CROSS → cross-cutting
    ("MISS", "CROSS",         "",   "",                            "Cross-cutting / Other"),
    # Any URL containing DIGITAL → Digital (broad; specific rules above take priority)
    ("DIGITAL",         None, "4",  "Digital, Industry and Space", "Digital, Industry & Space"),
    ("UCPM",            None, "",   "",                            "Cross-cutting / Other"),
    ("RFCS",            None, "5",  "Climate, Energy and Mobility","Climate, Energy & Mobility"),
    ("EUBA",            None, "",   "",                            "External Action & International Cooperation"),
    # PPPA: use subcode matching so CHIPS and MEDIA are distinguished correctly
    ("PPPA", "CHIPS",        "4",  "Digital, Industry and Space", "Digital, Industry & Space"),
    ("PPPA", "MEDIA",        "",   "",                            "Culture, Creativity & Inclusion"),
    ("PPPA",            None, "4",  "Digital, Industry and Space", "Digital, Industry & Space"),
    # Additional topic-details prefixes
    ("RENEWFM",         None, "5",  "Climate, Energy and Mobility", "Climate, Energy & Mobility"),
    ("SOCPL",           None, "",   "",                             "Culture, Creativity & Inclusion"),
    ("ERC",             None, "",   "",                             "Cross-cutting / Other"),
    ("EMFAF",           None, "6",  "Food, Bioeconomy, Natural Resources, Agriculture and Environment",
                                                                    "Food, Bioeconomy & Environment"),
    ("JUST",            None, "",   "",                             "Culture, Creativity & Inclusion"),
    ("I3",              None, "",   "",                             "SME, Entrepreneurship & Market Uptake"),
]

# Beneficiary overrides keyed by URL prefix
URL_BENEFICIARY_OVERRIDE = {
    "MSCA":  ["Research organisation"],
    "INFRA": ["Research organisation"],
    "EUAF":  ["Research organisation"],
    "EUBA":  ["Public body"],
}

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def norm(v):
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(v)).strip()


def _topic_id_from_url(url: str) -> str:
    """Return the topic identifier segment of a portal URL, uppercase, no query string."""
    s = norm(url).upper().split("?")[0]
    for marker in ["/TOPIC-DETAILS/", "/COMPETITIVE-CALLS-CS/"]:
        idx = s.find(marker)
        if idx >= 0:
            return s[idx + len(marker):]
    return s


def url_overrides(url: str):
    """
    Match URL against URL_RULES.
    Returns (cluster_num, cluster_label, thematic, beneficiary_or_None).
    """
    tid = _topic_id_from_url(url)
    if not tid:
        return ("", "", "", None)

    for entry in URL_RULES:
        prefix, subcode, c_num, c_label, thematic = entry
        if prefix not in tid:
            continue
        if subcode is not None:
            needle = f"-{subcode}-"
            if needle not in tid and not tid.split("?")[0].endswith(f"-{subcode}"):
                continue
        benef = URL_BENEFICIARY_OVERRIDE.get(prefix, None)
        return (c_num, c_label, thematic, benef)

    return ("", "", "", None)


def resolve_thematic(cluster_num: str, programme: str) -> str:
    if cluster_num:
        t = THEMATIC_MAP.get(cluster_num, "")
        if t:
            return t
    if programme:
        for key, label in PROGRAMME_THEMATIC_MAP.items():
            if key.lower() in programme.lower():
                return label
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
    if "digital" in sl:
        return "Digital Europe"
    if "defence fund" in sl or sl == "edf":
        return "EDF"
    if "single market" in sl or "smp" in sl:
        return "Single Market Programme"
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


def parse_date_to_iso(value):
    s = norm(value)
    if not s:
        return ""
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    m = re.search(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b", s)
    if m:
        month = MONTHS.get(m.group(2).lower())
        if month:
            try:
                return datetime(int(m.group(3)), month, int(m.group(1))).strftime("%Y-%m-%d")
            except ValueError:
                return ""
    return ""


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


def beneficiary_hint(record_type, action, programme, url_benef_override=None):
    if url_benef_override is not None:
        return url_benef_override

    rt = norm(record_type).lower()
    a  = norm(action).lower()
    p  = norm(programme).lower()
    hints = []

    if "competitive" in rt:
        hints.append("SME")
    if a == "ia":
        hints.extend(["SME", "Large enterprise", "Research organisation"])
    elif a == "ria":
        hints.extend(["Research organisation", "SME", "Large enterprise"])
    elif a == "csa":
        hints.extend(["Research organisation", "Public body", "NGO", "SME"])
    if "external action" in p:
        hints.extend(["NGO", "Public body", "Research organisation"])

    out, seen = [], set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def main():
    if not IN_XLSX.exists():
        raise SystemExit(f"File non trovato: {IN_XLSX.resolve()}")

    df = pd.read_excel(IN_XLSX)

    for col in ["Nome call", "Nome call (ID)", "Programma", "Cluster",
                "Tipo di azione", "Data inizio", "Data scadenza", "URL"]:
        if col not in df.columns:
            df[col] = ""
    for col in ["Tipo record", "Budget"]:
        if col not in df.columns:
            df[col] = ""

    calls = []
    for _, row in df.iterrows():
        name         = norm(row.get("Nome call"))
        call_id_raw  = norm(row.get("Nome call (ID)"))
        programme_raw= norm(row.get("Programma"))
        url          = norm(row.get("URL"))

        # Base cluster from spreadsheet columns
        # call_id_raw (Nome call (ID)) often contains HORIZON-CL[1-6]-... for
        # competitive-calls-cs entries where the Topic field encodes the cluster
        cluster_num = extract_cluster_num(
            row.get("Cluster"), name, call_id_raw, programme_raw, url
        )

        # URL-based overrides (missions + HLTH, EIC, CID, EURATOM, etc.)
        u_cnum, u_clabel, u_thematic, u_benef = url_overrides(url)
        if u_cnum:
            cluster_num = u_cnum
        cluster_label = u_clabel or CLUSTER_MAP.get(cluster_num, "")

        # Thematic cluster: URL override > THEMATIC_MAP > programme map
        if u_thematic:
            thematic = u_thematic
        else:
            thematic = resolve_thematic(cluster_num, normalize_programme(programme_raw))

        opening_raw  = norm(row.get("Data inizio"))
        deadline_raw = norm(row.get("Data scadenza"))

        call = {
            "name":            name,
            "call_id":         call_id_raw,
            "programme":       normalize_programme(programme_raw),
            "programme_raw":   programme_raw,
            "cluster_num":     cluster_num,
            "cluster_label":   cluster_label,
            "thematic_cluster":thematic,
            "action":          normalize_action(row.get("Tipo di azione")),
            "action_raw":      norm(row.get("Tipo di azione")),
            "opening":         opening_raw,
            "opening_iso":     parse_date_to_iso(opening_raw),
            "deadline":        deadline_raw,
            "deadline_iso":    parse_date_to_iso(deadline_raw),
            "budget":          norm(row.get("Budget")),
            "url":             url,
            "record_type":     norm(row.get("Tipo record")),
            "is_mission":      bool(u_cnum and "MISS" in _topic_id_from_url(url)),
            "beneficiary_hint":beneficiary_hint(
                row.get("Tipo record"),
                row.get("Tipo di azione"),
                row.get("Programma"),
                u_benef
            ),
        }

        if call["url"]:
            calls.append(call)

    uniq = {}
    for c in calls:
        uniq[c["url"]] = c

    payload = {"calls": list(uniq.values())}
    OUT_JSON.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"Creato {OUT_JSON.resolve()} con {len(payload['calls'])} call")

    # Diagnostics
    tcounts = {}
    for c in payload["calls"]:
        k = c.get("thematic_cluster") or "(non classificato)"
        tcounts[k] = tcounts.get(k, 0) + 1
    for k, v in sorted(tcounts.items(), key=lambda x: -x[1]):
        print(f"  {v:5d}  {k}")
    n_miss = sum(1 for c in payload["calls"] if c.get("is_mission"))
    print(f"Di cui HORIZON-MISS: {n_miss}")

    # ── Unclassified debug ────────────────────────────────────────────────────
    unclassified = [c for c in payload["calls"] if not c.get("thematic_cluster")]
    print(f"\nNon classificati: {len(unclassified)}")
    if unclassified:
        debug_path = Path("unclassified_calls.txt")
        lines = [f"Non classificati: {len(unclassified)}", ""]
        for c in unclassified:
            lines.append(c.get("url", "(no url)"))
        debug_path.write_text("\n".join(lines), encoding="utf-8")
        print(f"  → URL salvati in: {debug_path.resolve()}")


if __name__ == "__main__":
    main()