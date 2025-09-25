# ukids_scheduler_app.py
# uKids Scheduler — dynamic roles/capacities from Positions CSV headers + per-role '#' extra cap, starred overflow, Brooklyn rotation,
# P1 pre-pass, preferred leader rescue (Age 1→11 + Brooklyn babies/preschool leaders), latest responses only, 0=never, no double-booking, Excel export.
# NOTE: All Director-specific rules have been removed. Everyone has the same base cap.
# NEW: Adds an output that lists, per date, people who said YES but were not scheduled that day — with a second column
#      showing each person's role code (e.g., PSG, BSG). Also exported to Excel as paired columns per date.
# NEW (Sept 2025): Unscheduled table is sorted by campus (Pretoria, Nelspruit[ Nel ], Polokwane[ POL ], Tygerberg[ TGB ]) based on codes column.

import io
import re
import base64
from collections import defaultdict, Counter
from datetime import datetime, date

import pandas as pd
import streamlit as st
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

# ──────────────────────────────────────────────────────────────────────────────
# Streamlit chrome
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="uKids Scheduler", layout="wide")
st.title("uKids Scheduler")

st.markdown(
    """
    <style>
      .stApp { background: #000; color: #fff; }
      .stButton>button, .stDownloadButton>button { background:#444; color:#fff; }
      .stDataFrame { background:#111; }
      .stAlert { color:#111; }
    </style>
    """,
    unsafe_allow_html=True,
)

# Optional logo (ignore if missing)
for logo_name in ["image(1).png", "image.png", "logo.png"]:
    try:
        with open(logo_name, "rb") as f:
            encoded = base64.b64encode(f.read()).decode()
        st.markdown(
            f"<div style='text-align:center'><img src='data:image/png;base64,{encoded}' width='520'></div>",
            unsafe_allow_html=True,
        )
        break
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# Constants & helpers
# ──────────────────────────────────────────────────────────────────────────────
MONTH_ALIASES = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}
YES_SET = {"yes", "y", "true", "available"}

# Remove these roles entirely (match on base label)
EXCLUDED_ROLES = {"entrance greeter"}

# Special gates (normalized names) — keep if you ever need a leader badge for certain roles
REQUIRES_LEADER = {"helping ninja and check in leader"}

# Strict per-Sunday preferred-fill for rescue
# UPDATED: Prioritize these roles (in this exact order) before all others
PREFERRED_FILL_ORDER = [
    "age 1 leader",
    "age 2 leader",
    "age 3 leader",
    "age 4 leader",
    "age 5 leader",
    "age 6 leader",
    "age 7 leader",
    "age 8 leader",
    "age 9 leader",
    "age 10 leader",
    "age 11 leader",
    "brooklyn babies leader",
    "brooklyn preschool leader",
]
PREFERRED_INDEX = {r: i for i, r in enumerate(PREFERRED_FILL_ORDER)}

# Recognized short codes as they appear in column 2 (role codes)
KNOWN_CODES_ORDER = [
    "BSG", "PSG", "ESG", "DSG",  # serving girls
    "BL", "PL", "EL", "SL",      # leaders
    "UL", "USG",                 # uGroup
    "D",                         # director (no special rules)
]

# Campus parsing (from codes column)
CAMPUS_ORDER = ["pretoria", "nelspruit", "polokwane", "tygerberg"]
CAMPUS_LABELS = {"pretoria": "Pretoria", "nelspruit": "Nelspruit", "polokwane": "Polokwane", "tygerberg": "Tygerberg"}

def normalize(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip()

def norm_name(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def sanitize_header_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ").replace("×", "x")
    return s.strip()

CAP_PATTERNS = [
    re.compile(r"^(?P<base>.*?)[\s\-]*\(\s*x?\s*(?P<n>\d+)\s*\)\s*$", re.IGNORECASE),
    re.compile(r"^(?P<base>.*?)[\s\-]*\[\s*x?\s*(?P<n>\d+)\s*\]\s*$", re.IGNORECASE),
    re.compile(r"^(?P<base>.*?)[\s\-]*x\s*(?P<n>\d+)\s*$", re.IGNORECASE),
]

def parse_role_meta(header: str):
    """
    From a Positions header return (base_label, capacity:int, is_starred:bool).
    Accepts variations: 'Role* (x3)', 'Role (x3)', 'Role* x3', 'Role x3', 'Role*'.
    """
    s = sanitize_header_text(header)
    base = s
    cap = 1
    for pat in CAP_PATTERNS:
        m = pat.match(s)
        if m:
            base = sanitize_header_text(m.group("base"))
            cap = max(1, int(m.group("n")))
            break
    starred = False
    if base.endswith("*"):
        starred = True
        base = base[:-1].rstrip()
    return base, cap, starred


def strip_capacity_tag(role_label: str) -> str:
    base, _, _ = parse_role_meta(role_label)
    return base


def read_csv_robust(uploaded_file, label_for_error):
    raw = uploaded_file.getvalue()
    encodings = ["utf-8", "utf-8-sig", "cp1252", "iso-8859-1"]
    seps = [None, ",", ";", "\t", "|"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(io.BytesIO(raw), encoding=enc, engine="python", sep=sep)
                if df.shape[1] == 0:
                    raise ValueError("Parsed 0 columns.")
                return df
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                continue
    st.error(
        f"Could not read {label_for_error} CSV. Last error: {last_err}. "
        "Try re-exporting as CSV (UTF-8) or remove unusual characters in headers."
    )
    st.stop()


def read_table_any(uploaded_file, label_for_error):
    name = uploaded_file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        try:
            return pd.read_excel(uploaded_file)
        except Exception:
            st.error(f"Could not read {label_for_error} Excel file. Please save as CSV or check the format.")
            st.stop()
    else:
        return read_csv_robust(uploaded_file, label_for_error)


def detect_name_column(df: pd.DataFrame, fallback_first: bool = True) -> str:
    candidates = [
        "What is your name AND surname?",
        "What is your name and surname?",
        "Name & Surname",
        "Name and Surname",
        "Name",
        "Full name",
        "Full names",
    ]
    cols_l = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        key = c.strip().lower()
        if key in cols_l:
            return cols_l[key]
    for c in df.columns:
        if isinstance(c, str) and "name" in c.lower():
            return c
    if fallback_first:
        return df.columns[0]
    raise ValueError("Could not detect a 'name' column.")


def is_priority_col(series: pd.Series) -> bool:
    """
    Determine if a column is a priority column even if cells contain things like '2#'.
    We extract first integer from each non-empty cell and test range 0..5.
    """
    vals = []
    for v in series.dropna():
        s = str(v).strip()
        m = re.search(r"\d+", s)
        if m:
            n = int(m.group(0))
            vals.append(n)
    if not vals:
        return False
    return (min(vals) >= 0) and (max(vals) <= 5)


def parse_month_and_dates_from_headers(responses_df: pd.DataFrame):
    # Accept "Are you available ..." OR "05-Oct" / "5 Oct" / "5-October"
    avail_cols = [c for c in responses_df.columns if isinstance(c, str) and c.strip().lower().startswith("are you available")]
    if not avail_cols:
        avail_cols = [
            c for c in responses_df.columns
            if isinstance(c, str) and re.search(r"\b(\d{1,2})\s*[-/ ]\s*([A-Za-z]{3,})\b", c)
        ]
    if not avail_cols:
        raise ValueError("No availability columns found. Use headers like '05-Oct' or 'Are you available 7 September?'")

    def parse_day_month(col):
        m = re.search(r"\b(\d{1,2})\s*[-/ ]\s*([A-Za-z]{3,})\b", col)
        if not m:
            return None
        day = int(m.group(1))
        mon_txt = m.group(2).lower()[:3]
        month = MONTH_ALIASES.get(mon_txt)
        return day, month

    info = []
    for c in avail_cols:
        res = parse_day_month(str(c))
        if res:
            d, m = res
            info.append((c, m, d))

    months = {m for _, m, _ in info if m is not None}
    if not months:
        raise ValueError("Could not parse month from availability headers.")
    if len(months) > 1:
        raise ValueError(f"Multiple months detected in availability headers: {sorted(months)}. Upload one month at a time.")
    month = months.pop()

    if "Timestamp" in responses_df.columns:
        years = pd.to_datetime(responses_df["Timestamp"], errors="coerce").dt.year.dropna().astype(int)
        year = int(years.mode().iloc[0]) if not years.empty else date.today().year
    else:
        year = date.today().year

    date_map = {c: pd.Timestamp(datetime(year, month, d)).normalize() for c, m, d in info if m is not None}
    service_dates = sorted(set(date_map.values()))
    sheet_name = f"{pd.Timestamp(year=year, month=month, day=1):%B %Y}"
    return year, month, date_map, service_dates, sheet_name

# ──────────────────────────────────────────────────────────────────────────────
# Data shaping
# ──────────────────────────────────────────────────────────────────────────────

def build_display_name_map(positions_df: pd.DataFrame, name_col: str):
    disp = {}
    for _, r in positions_df.iterrows():
        raw = str(r[name_col]).strip()
        if raw:
            disp.setdefault(norm_name(raw), raw)
    return disp


def extract_primary_code(raw: str) -> str:
    """Return the first recognized short code from a raw codes cell."""
    toks = re.findall(r"[A-Za-z]+", str(raw or "").upper())
    for code in KNOWN_CODES_ORDER:
        if code in toks:
            return code
    return toks[0] if toks else ""


def build_person_code_map(positions_df: pd.DataFrame, name_col: str, codes_col: str | None):
    code_map = {}
    if codes_col and codes_col in positions_df.columns:
        for _, r in positions_df.iterrows():
            nm = norm_name(str(r[name_col]).strip())
            if not nm:
                continue
            code_map[nm] = extract_primary_code(r.get(codes_col, ""))
    return code_map


def build_person_campus_map(positions_df: pd.DataFrame, name_col: str, codes_col: str | None):
    """
    Detect campus from codes column tokens:
    - If contains 'NEL' => Nelspruit
    - If contains 'POL' => Polokwane
    - If contains 'TGB' => Tygerberg
    - Else => Pretoria
    """
    camp_map = {}
    if codes_col and codes_col in positions_df.columns:
        for _, r in positions_df.iterrows():
            nm = norm_name(str(r[name_col]).strip())
            if not nm:
                continue
            raw = str(r.get(codes_col, "") or "").upper()
            tokens = re.findall(r"[A-Z]+", raw)
            campus = "pretoria"
            if any(t == "NEL" for t in tokens):
                campus = "nelspruit"
            elif any(t == "POL" for t in tokens):
                campus = "polokwane"
            elif any(t == "TGB" for t in tokens):
                campus = "tygerberg"
            camp_map[nm] = campus
    return camp_map


def build_long_df(people_df: pd.DataFrame, name_col: str, role_cols, codes_col: str = None):
    """
    Returns:
      - long_df rows: {person (norm), role (header as-is), priority:int}
      - role_codes per person:
          raw, has_BL/PL/EL/SL flags (kept if you still use leader gates), extra_roles_norm=set of normalized base roles where the cell had a '#'
    """
    records = []
    role_codes = {}
    for _, r in people_df.iterrows():
        display_name = str(r[name_col]).strip()
        if not display_name or display_name.lower() == "nan":
            continue
        person = norm_name(display_name)

        flags = {"raw": "", "has_BL": False, "has_PL": False, "has_EL": False, "has_SL": False, "extra_roles_norm": set()}
        if codes_col and codes_col in people_df.columns:
            raw = str(r.get(codes_col, "") or "")
            flags["raw"] = raw
            toks = re.findall(r"[A-Za-z]+", raw.upper())
            for t in toks:
                if t == "BL": flags["has_BL"] = True
                elif t == "PL": flags["has_PL"] = True
                elif t == "EL": flags["has_EL"] = True
                elif t == "SL": flags["has_SL"] = True

        for role_hdr in role_cols:
            cell_raw = r.get(role_hdr, "")
            cell = "" if pd.isna(cell_raw) else str(cell_raw).strip()
            if cell == "":
                continue
            has_hash = "#" in cell
            m = re.search(r"\d+", cell)
            if not m:
                continue
            pr = int(m.group(0))
            if pr >= 1:
                records.append({"person": person, "role": role_hdr, "priority": pr})
                if has_hash:
                    flags["extra_roles_norm"].add(normalize(strip_capacity_tag(role_hdr)))

        role_codes[person] = flags

    return pd.DataFrame(records), role_codes


def dedupe_latest_by_key(df: pd.DataFrame, key_series: pd.Series) -> pd.DataFrame:
    key_norm = key_series.map(norm_name)
    df2 = df.assign(_key=key_norm)
    if "Timestamp" in df2.columns:
        ts = pd.to_datetime(df2["Timestamp"], errors="coerce")
        df2 = df2.assign(_ts=ts).sort_values("_ts")
        latest = df2.groupby("_key", as_index=False).tail(1).drop(columns=["_ts"])
        return latest
    return df2.groupby("_key", as_index=False).tail(1)


def parse_availability(responses_df: pd.DataFrame, name_col_resp: str, date_map):
    responses_latest = dedupe_latest_by_key(responses_df, responses_df[name_col_resp])

    availability = {}
    yes_counts = Counter()
    display_from_responses = {}

    for _, row in responses_latest.iterrows():
        disp_name = str(row.get(name_col_resp, "")).strip()
        key = str(row.get("_key", "")).strip()
        if not key or key.lower() == "nan":
            continue
        display_from_responses[key] = disp_name

        availability.setdefault(key, {})
        for col, dt in date_map.items():
            ans = str(row.get(col, "")).strip().lower()
            is_yes = ans in YES_SET
            availability[key][dt] = is_yes
            if is_yes:
                yes_counts[key] += 1

    few_yes = sorted([n for n, c in yes_counts.items() if c < 2])
    service_dates = sorted(set(date_map.values()))
    return availability, service_dates, few_yes, display_from_responses


def build_slot_plan_dynamic(all_role_headers):
    slot_plan = {}
    starred_norm = set()
    excluded_norm = {normalize(x) for x in EXCLUDED_ROLES}
    for hdr in all_role_headers:
        base_label, cap, starred = parse_role_meta(hdr)
        if normalize(base_label) in excluded_norm:
            continue
        slot_plan[base_label] = max(cap, slot_plan.get(base_label, 0))
        if starred:
            starred_norm.add(normalize(base_label))
    return slot_plan, starred_norm


def expand_roles_to_slots(slot_plan):
    slot_rows = []
    slot_index = {}
    for role, n in slot_plan.items():
        if n <= 0:
            continue
        if n == 1:
            lab = role
            slot_rows.append(lab)
            slot_index[lab] = role
        else:
            for i in range(1, n + 1):
                lab = f"{role} #{i}"
                slot_rows.append(lab)
                slot_index[lab] = role
    return slot_rows, slot_index


def build_eligibility(long_df: pd.DataFrame):
    elig = defaultdict(set)
    for _, r in long_df.iterrows():
        elig[str(r["person"]).strip()].add(str(r["role"]).strip())
    return elig


def build_priority_lookup(long_df: pd.DataFrame):
    lut = {}
    for _, r in long_df.iterrows():
        base = strip_capacity_tag(str(r["role"]))
        lut[(str(r["person"]).strip(), normalize(base))] = int(r["priority"])
    return lut


def is_ukids_leader(flags: dict) -> bool:
    # Keep this if certain roles require a leader; otherwise return True to disable gate entirely
    return bool(flags.get("has_BL") or flags.get("has_PL") or flags.get("has_EL") or flags.get("has_SL"))


def base_cap_for_person(flags: dict) -> int:
    # Everyone has the same base monthly cap now.
    return 2


def role_allowed_for_person(eligibility, person_norm, base_role):
    nb = normalize(strip_capacity_tag(base_role))
    for er in eligibility.get(person_norm, set()):
        if normalize(strip_capacity_tag(er)) == nb:
            return True
    return False


def pref_sort_key(pref_val):
    # Prefer 2 > 3 > 4 > 1 (P1 handled separately)
    if pref_val == 2: return 0
    if pref_val == 3: return 1
    if pref_val == 4: return 2
    if pref_val == 1: return 3
    return 9


def get_priority_for(lookup, person_norm, role_name):
    return lookup.get((person_norm, normalize(strip_capacity_tag(role_name))))


def compute_p1_roles_by_person(long_df, allowed_roles_set):
    p1 = defaultdict(set)
    allowed_norm = {normalize(strip_capacity_tag(r)) for r in allowed_roles_set}
    for _, r in long_df.iterrows():
        base = strip_capacity_tag(str(r["role"]).strip())
        if normalize(base) not in allowed_norm:
            continue
        if int(r["priority"]) == 1:
            p1[str(r["person"]).strip()].add(base)
    return p1


def served_in_priority_one(schedule_cells, p1_roles_by_person, slot_to_role):
    served = set()
    for (row_name, _d), names in schedule_cells.items():
        base_role = slot_to_role.get(row_name, row_name)
        for nm_norm in names:
            if base_role in p1_roles_by_person.get(nm_norm, set()):
                served.add(nm_norm)
    return served


def recompute_assign_counts(schedule_cells):
    cnt = Counter()
    for (_row, _d), names in schedule_cells.items():
        for nm in names:
            cnt[nm] += 1
    return cnt

# ─────────────── Brooklyn exclusions (generalized) ───────────────

def parse_brooklyn_exclusions(excl_df: pd.DataFrame, positions_headers):
    """
    Treat ANY listed name as excluded from ALL Brooklyn roles this month.
    Returns: (is_excluded_fn, summary_text, excluded_names_set)
    """
    if excl_df is None or excl_df.empty:
        return (lambda person_norm, base_role: False, "No exclusions.", set())

    # detect name column
    name_col = None
    for cand in ["Name & Surname", "Name and Surname", "Name", "Full name", "Full names"]:
        if cand in excl_df.columns:
            name_col = cand
            break
    if name_col is None:
        name_col = excl_df.columns[0]

    excluded_names = set()
    for _, r in excl_df.iterrows():
        nm = norm_name(str(r.get(name_col, "")).strip())
        if nm:
            excluded_names.add(nm)

    def is_excluded(person_norm, base_role):
        nb = normalize(strip_capacity_tag(base_role))
        return ("brooklyn" in nb) and (person_norm in excluded_names)

    summary = f"Exclusions loaded: {len(excluded_names)} people blocked from ALL Brooklyn roles."
    return is_excluded, summary, excluded_names


def brooklyn_exclusion_prefer_p1_and_swap(schedule_cells, slot_rows, slot_to_role, service_dates,
                                          availability, role_codes, eligibility, is_excluded,
                                          p1_roles_by_person, people, requires_leader_norm):
    def nb(s): return normalize(strip_capacity_tag(s))

    def eligible_and_available(person, base, d):
        if not availability.get(person, {}).get(d, False):
            return False
        if not role_allowed_for_person(eligibility, person, base):
            return False
        if nb(base) in requires_leader_norm and not is_ukids_leader(role_codes.get(person, {})):
            return False
        return True

    def person_assigned_on_date(person, d):
        return any(person in names for (rn, dd), names in schedule_cells.items() if dd == d)

    def find_free_candidate_for(base_role, d, forbidden=set()):
        pool = []
        for q in people:
            if q in forbidden:
                continue
            if person_assigned_on_date(q, d):
                continue
            if not eligible_and_available(q, base_role, d):
                continue
            pool.append(q)
        if not pool:
            return None
        pool.sort()
        return pool[0]

    for d in service_dates:
        brook_rows = [row for row in slot_rows if "brooklyn" in nb(slot_to_role[row])]
        for row in brook_rows:
            if not schedule_cells[(row, d)]:
                continue
            p = schedule_cells[(row, d)][0]
            if not is_excluded(p, slot_to_role[row]):
                continue

            p1_set = p1_roles_by_person.get(p, set())

            # A) Swap into their P1
            swapped = False
            for r2 in slot_rows:
                base2 = slot_to_role[r2]
                if base2 not in p1_set:
                    continue
                if len(schedule_cells[(r2, d)]) == 0:
                    continue
                q = schedule_cells[(r2, d)][0]
                if not eligible_and_available(q, slot_to_role[row], d):
                    continue
                if not eligible_and_available(p, base2, d):
                    continue
                schedule_cells[(row, d)][0] = q
                schedule_cells[(r2, d)][0] = p
                swapped = True
                break
            if swapped:
                continue

            # B) Move into empty P1 + backfill Brooklyn
            empty_p1_rows = [r2 for r2 in slot_rows if (slot_to_role[r2] in p1_set) and len(schedule_cells[(r2, d)]) == 0]
            moved = False
            for r2 in empty_p1_rows:
                base2 = slot_to_role[r2]
                if not eligible_and_available(p, base2, d):
                    continue
                replacement = find_free_candidate_for(slot_to_role[row], d, forbidden={p})
                if replacement and not is_excluded(replacement, slot_to_role[row]):
                    schedule_cells[(r2, d)].append(p)
                    schedule_cells[(row, d)][0] = replacement
                    moved = True
                    break

# ─────────────── Per-role '#' helper ───────────────

def within_role_specific_cap(person, target_base_role, flags, assign_count):
    """
    Base caps: everyone=2.
    Allow at most +1 total beyond base — but only if this extra assignment
    is INTO a role marked with '#' for this person (extra_roles_norm).
    """
    base_cap = base_cap_for_person(flags)
    tot = assign_count.get(person, 0)
    if tot < base_cap:
        return True
    nb = normalize(strip_capacity_tag(target_base_role))
    return (tot < base_cap + 1) and (nb in flags.get("extra_roles_norm", set()))

# ──────────────────────────────────────────────────────────────────────────────
# Scheduling core
# ──────────────────────────────────────────────────────────────────────────────

def main_pass_schedule(long_df, availability, service_dates, role_codes, all_role_headers, exclusions_tuple):
    is_excluded, _excl_summary, excluded_names_set = exclusions_tuple

    # Build slots and starred set
    slot_plan, starred_norm_set = build_slot_plan_dynamic(all_role_headers)
    slot_rows, slot_to_role = expand_roles_to_slots(slot_plan)

    # Eligibility & priorities
    eligibility = build_eligibility(long_df)
    priority_lut = build_priority_lookup(long_df)

    # Only people present in BOTH sources
    people = sorted(set(eligibility.keys()) & set(availability.keys()))

    # Storage
    schedule_cells = {(slot, d): [] for slot in slot_rows for d in service_dates}
    assign_count = defaultdict(int)

    def slot_sort_key(s):
        base_role = slot_to_role[s]
        n = normalize(base_role)
        if n in PREFERRED_INDEX:
            return (0, PREFERRED_INDEX[n], s.lower())
        s_low = s.lower()
        if "leader" in s_low:
            return (1, s_low)
        if "classroom" in s_low:
            return (2, s_low)
        return (3, s_low)

    slot_rows_sorted = sorted(slot_rows, key=slot_sort_key)

    # P1 pre-pass — prioritize Brooklyn-excluded names first
    p1_roles_by_person = compute_p1_roles_by_person(long_df, allowed_roles_set=slot_plan.keys())
    avail_count = {p: sum(1 for d in service_dates if availability.get(p, {}).get(d, False)) for p in people}
    p1_people = [p for p in people if p1_roles_by_person.get(p)]
    p1_people_order = sorted(
        p1_people,
        key=lambda p: (0 if p in excluded_names_set else 1, avail_count.get(p, 0), p)
    )

    for p in p1_people_order:
        flags = role_codes.get(p, {})
        got_one = False
        for d in service_dates:
            if not availability.get(p, {}).get(d, False):
                continue
            if any(p in names for (rn, dd), names in schedule_cells.items() if dd == d):
                continue
            for slot_row in slot_rows_sorted:
                base_role = slot_to_role[slot_row]
                if base_role not in p1_roles_by_person[p]:
                    continue
                if is_excluded(p, base_role):
                    continue
                if not role_allowed_for_person(eligibility, p, base_role):
                    continue
                if normalize(base_role) in REQUIRES_LEADER and not is_ukids_leader(flags):
                    continue
                if not within_role_specific_cap(p, base_role, flags, assign_count):
                    continue
                if len(schedule_cells[(slot_row, d)]) == 0:
                    schedule_cells[(slot_row, d)].append(p)
                    assign_count[p] += 1
                    got_one = True
                    break
            if got_one:
                break

    # General fill — respects Brooklyn exclusion and per-role '#'
    for d in service_dates:
        assigned_today = set(nm for (rn, dd), names in schedule_cells.items() if dd == d for nm in names)
        for slot_row in slot_rows_sorted:
            base_role = slot_to_role[slot_row]
            if len(schedule_cells[(slot_row, d)]) >= 1:
                continue

            cands = []
            for p in people:
                flags = role_codes.get(p, {})
                if p in assigned_today:
                    continue
                if not availability.get(p, {}).get(d, False):
                    continue
                if is_excluded(p, base_role):
                    continue
                if not role_allowed_for_person(eligibility, p, base_role):
                    continue
                if normalize(base_role) in REQUIRES_LEADER and not is_ukids_leader(flags):
                    continue
                if not within_role_specific_cap(p, base_role, flags, assign_count):
                    continue
                pr = get_priority_for(priority_lut, p, base_role)
                cands.append((p, pr))

            if cands:
                cands.sort(key=lambda t: (assign_count[t[0]], (pref_sort_key(t[1]) if t[1] is not None else 9), t[0]))
                chosen = cands[0][0]
                schedule_cells[(slot_row, d)].append(chosen)
                assign_count[chosen] += 1
                assigned_today.add(chosen)   # keep same-day unique

    # Brooklyn swap/move pass — ensure excluded people do not stay in Brooklyn
    brooklyn_exclusion_prefer_p1_and_swap(
        schedule_cells, slot_rows, slot_to_role, service_dates,
        availability, role_codes, eligibility, is_excluded,
        p1_roles_by_person, people, requires_leader_norm=REQUIRES_LEADER
    )

    assign_count = recompute_assign_counts(schedule_cells)

    # Preferred roles rescue (UPDATED LIST)
    def pref_rank(base_role):
        return PREFERRED_INDEX.get(normalize(base_role), 999)

    def find_free_candidate_for(base_role, d, assigned_today_set):
        pool = []
        for p in people:
            flags = role_codes.get(p, {})
            if p in assigned_today_set:
                continue
            if not availability.get(p, {}).get(d, False):
                continue
            if is_excluded(p, base_role):
                continue
            if not role_allowed_for_person(eligibility, p, base_role):
                continue
            if normalize(base_role) in REQUIRES_LEADER and not is_ukids_leader(flags):
                continue
            if not within_role_specific_cap(p, base_role, flags, assign_count):
                continue
            pr = get_priority_for(priority_lut, p, base_role)
            pool.append((p, pr))
        if not pool:
            return None
        pool.sort(key=lambda t: ((pref_sort_key(t[1]) if t[1] is not None else 9), assign_count[t[0]], t[0]))
        return pool[0][0]

    for d in service_dates:
        assigned_today = {nm for (rn, dd), names in schedule_cells.items() if dd == d for nm in names}

        for pref_base in PREFERRED_FILL_ORDER:
            target_rows = [row for row in slot_rows if normalize(slot_to_role[row]) == pref_base]
            for row in target_rows:
                if len(schedule_cells[(row, d)]) >= 1:
                    continue

                cand = find_free_candidate_for(slot_to_role[row], d, assigned_today)
                if cand:
                    schedule_cells[(row, d)].append(cand)
                    assign_count[cand] += 1
                    assigned_today.add(cand)
                    continue

                donor_cells = []
                for (r2, dd), names in schedule_cells.items():
                    if dd != d or not names:
                        continue
                    base2 = slot_to_role[r2]
                    if pref_rank(base2) > pref_rank(slot_to_role[row]):
                        donor_cells.append((r2, base2, names[0]))

                for r2, base2, person in donor_cells:
                    flags = role_codes.get(person, {})
                    if is_excluded(person, slot_to_role[row]):
                        continue
                    if not role_allowed_for_person(eligibility, person, slot_to_role[row]):
                        continue
                    if normalize(slot_to_role[row]) in REQUIRES_LEADER and not is_ukids_leader(flags):
                        continue
                    temp_assigned = assigned_today - {person}
                    backfill = find_free_candidate_for(base2, d, temp_assigned)
                    if backfill is None:
                        continue
                    schedule_cells[(r2, d)].remove(person)
                    schedule_cells[(row, d)].append(person)
                    schedule_cells[(r2, d)].append(backfill)
                    assign_count[backfill] += 1
                    assigned_today.add(backfill)
                    break

    assign_count = recompute_assign_counts(schedule_cells)

    # Star overflow pass — allow +1 (total) in STARRED roles only (after non-star cap)
    slot_plan_tmp, starred_norm_set = build_slot_plan_dynamic(all_role_headers)

    for d in service_dates:
        assigned_today = {nm for (rn, dd), names in schedule_cells.items() if dd == d for nm in names}

        total = Counter()
        nonstar = Counter()
        for (row, dd), names in schedule_cells.items():
            base = slot_to_role[row]
            is_star = normalize(base) in starred_norm_set
            for nm in names:
                total[nm] += 1
                if not is_star:
                    nonstar[nm] += 1

        starred_empty_rows = [
            row for row in slot_rows
            if (normalize(slot_to_role[row]) in starred_norm_set) and (len(schedule_cells[(row, d)]) == 0)
        ]

        for row in starred_empty_rows:
            base_role = slot_to_role[row]
            cands = []
            for p in people:
                flags = role_codes.get(p, {})
                if p in assigned_today:
                    continue
                if not availability.get(p, {}).get(d, False):
                    continue
                if is_excluded(p, base_role):
                    continue
                if not role_allowed_for_person(eligibility, p, base_role):
                    continue
                if normalize(base_role) in REQUIRES_LEADER and not is_ukids_leader(flags):
                    continue

                base_cap = base_cap_for_person(flags)
                tot = total.get(p, 0)
                non = nonstar.get(p, 0)

                allow_normal = tot < base_cap
                allow_overflow = (non >= base_cap) and (tot < base_cap + 1)

                if allow_normal or allow_overflow:
                    pr = get_priority_for(priority_lut, p, base_role)
                    cands.append((p, pr, tot))

            if cands:
                cands.sort(key=lambda t: (t[2], (pref_sort_key(t[1]) if t[1] is not None else 9), t[0]))
                chosen = cands[0][0]
                schedule_cells[(row, d)].append(chosen)
                assign_count[chosen] = assign_count.get(chosen, 0) + 1
                assigned_today.add(chosen)   # keep same-day unique in overflow

    return schedule_cells, assign_count, slot_rows, slot_to_role, eligibility, people, p1_roles_by_person

# ──────────────────────────────────────────────────────────────────────────────
# Output helpers
# ──────────────────────────────────────────────────────────────────────────────

def excel_autofit(ws):
    for col_idx, column_cells in enumerate(
        ws.iter_cols(min_row=1, max_row=ws.max_row, min_col=1, max_col=ws.max_column), start=1
    ):
        max_len = 0
        for cell in column_cells:
            val = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(val))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max(12, max_len + 2), 80)


def build_schedule_df(schedule_cells, slot_rows, service_dates, name_display_map):
    cols = [d.strftime("%Y-%m-%d") for d in service_dates]
    df = pd.DataFrame(index=slot_rows, columns=cols)
    for (slot_row, d), names in schedule_cells.items():
        disp = [name_display_map.get(nm, nm) for nm in names]
        df.loc[slot_row, d.strftime("%Y-%m-%d")] = ", ".join(disp)
    return df.fillna("")


def build_person_assignment_details(schedule_cells, name_display_map):
    per = defaultdict(list)
    for (slot_row, d), names in schedule_cells.items():
        for nm_norm in names:
            per[nm_norm].append((d, slot_row))
    details = {}
    for nm_norm, items in per.items():
        items_sorted = sorted(items, key=lambda x: x[0])
        disp_name = name_display_map.get(nm_norm, nm_norm)
        details[disp_name] = "; ".join([f"{dt.strftime('%Y-%m-%d')} — {slot}" for dt, slot in items_sorted])
    return details


def build_unscheduled_available(schedule_cells, service_dates, availability, name_display_map, code_map, campus_map):
    """
    Return a DataFrame with two columns per date: Name and Code for everyone
    who said YES on that date but was not scheduled anywhere that day.
    Sorted by campus (Pretoria, Nelspruit, Polokwane, Tygerberg), then by name.
    """
    scheduled_by_date = defaultdict(set)
    for (slot_row, d), names in schedule_cells.items():
        for nm in names:
            scheduled_by_date[d].add(nm)

    def campus_key(person_norm):
        c = campus_map.get(person_norm, "pretoria")
        # Map to index for stable ordering
        try:
            idx = CAMPUS_ORDER.index(c)
        except ValueError:
            idx = 0
        return idx

    per_date_names = {}
    per_date_codes = {}
    for d in service_dates:
        yes_people = [p for p in availability.keys() if availability.get(p, {}).get(d, False)]
        unscheduled = [p for p in yes_people if p not in scheduled_by_date.get(d, set())]

        # Compose tuples for sorting: (campusIndex, displayName, code, person_norm)
        tuples = []
        for p in unscheduled:
            disp = name_display_map.get(p, p)
            code = code_map.get(p, "")
            ck = campus_key(p)
            tuples.append((ck, disp or "", code, p))

        tuples.sort(key=lambda t: (t[0], t[1]))  # by campus then name

        per_date_names[d] = [t[1] for t in tuples]
        per_date_codes[d] = [t[2] for t in tuples]

    # pad ragged lists
    max_len = max((len(v) for v in per_date_names.values()), default=0)
    data = {}
    for d in service_dates:
        col_name = d.strftime("%Y-%m-%d")
        col_code = f"{col_name} code"
        names_list = per_date_names[d] + [""] * (max_len - len(per_date_names[d]))
        codes_list = per_date_codes[d] + [""] * (max_len - len(per_date_codes[d]))
        data[col_name] = names_list
        data[col_code] = codes_list

    return pd.DataFrame(data)

# ──────────────────────────────────────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────────────────────────────────────

st.subheader("1) Upload files")
c1, c2, c3 = st.columns(3)
with c1:
    positions_file = st.file_uploader("Serving positions (CSV)", type=["csv"], key="positions_csv_any")
with c2:
    responses_file = st.file_uploader("Availability responses (CSV)", type=["csv"], key="responses_csv_any")
with c3:
    exclusions_file = st.file_uploader("Last month Brooklyn exclusions (CSV/XLSX)", type=["csv", "xlsx", "xls"], key="exclusions_csv_any")

st.caption("• Positions CSV: first col = volunteer names; second col = optional role codes (BL/PL/EL/SL/BSG/PSG/ESG/DSG + campus tags like NEL/POL/TGB). Other cols = roles with values 0–5.")
st.caption("• To give someone +1 extra assignment but ONLY for a specific role, put a '#' in that cell, e.g. '2#'.")
st.caption("• For capacity add '(xN)' / 'xN' / '[xN]' to headers. Add '*' at end to mark starred roles (eligible for +1 overflow after base cap).")
st.caption("• Responses CSV: has a name column + availability columns like '05-Oct' or 'Are you available 7 September?' and a Timestamp.")
st.caption("• Exclusions (optional): List of names who served in Brooklyn last month. They’re blocked from all Brooklyn roles and get P1 priority.")
st.caption("• Priority rescue order: Age 1→11 Leaders, then Brooklyn Babies Leader, then Brooklyn Preschool Leader.")
st.caption("• Unscheduled sorting: Pretoria → Nelspruit (Nel) → Polokwane (POL) → Tygerberg (TGB). Detected from the codes column tokens NEL/POL/TGB.")

if st.button("Generate Schedule", type="primary"):
    if not positions_file or not responses_file:
        st.error("Please upload the Positions and Responses files.")
        st.stop()

    positions_df = read_csv_robust(positions_file, "positions")
    responses_df = read_csv_robust(responses_file, "responses")
    excl_df = None
    if exclusions_file is not None:
        excl_df = read_table_any(exclusions_file, "exclusions")

    # Detect name columns
    try:
        name_col_positions = positions_df.columns[0]
    except Exception as e:
        st.error(f"Could not detect a name column in Positions CSV: {e}")
        st.stop()
    try:
        name_col_responses = detect_name_column(responses_df, fallback_first=False)
    except Exception as e:
        st.error(f"Could not detect a name column in Responses CSV: {e}")
        st.stop()

    codes_col = positions_df.columns[1] if positions_df.shape[1] >= 2 else None
    positions_df[name_col_positions] = positions_df[name_col_positions].astype(str)
    responses_df[name_col_responses] = responses_df[name_col_responses].astype(str)

    # Display name map (prefer Positions casing)
    name_display_map = build_display_name_map(positions_df, name_col_positions)

    # Code map + Campus map for the unscheduled table
    code_map = build_person_code_map(positions_df, name_col_positions, codes_col)
    campus_map = build_person_campus_map(positions_df, name_col_positions, codes_col)

    # Roles from 3rd column onward (accept '2#' etc.)
    raw_role_cols = [c for c in positions_df.columns[2:] if is_priority_col(positions_df[c])]
    excluded_norm = {normalize(x) for x in EXCLUDED_ROLES}
    role_cols = [c for c in raw_role_cols if normalize(strip_capacity_tag(c)) not in excluded_norm]
    if not role_cols:
        st.error("No usable role columns detected in Positions CSV (from the third column onwards).")
        st.stop()

    # Brooklyn exclusions checker
    is_excluded, excl_summary, brooklyn_excluded_names = parse_brooklyn_exclusions(excl_df, positions_headers=role_cols)

    # Eligibility & codes
    long_df, role_codes = build_long_df(positions_df, name_col_positions, role_cols, codes_col=codes_col)
    if long_df.empty:
        st.error("No eligible assignments found (after removing 0s).")
        st.stop()

    # Dates
    try:
        year, month, date_map, service_dates, sheet_name = parse_month_and_dates_from_headers(responses_df)
    except Exception as e:
        st.error(f"Could not parse month & dates from responses: {e}")
        st.stop()

    # Availability (latest per person)
    availability, service_dates, few_yes_list_norm, display_from_responses = parse_availability(responses_df, name_col_responses, date_map)
    for k, disp in display_from_responses.items():
        name_display_map.setdefault(k, disp)

    # Schedule
    schedule_cells, assign_count_norm, slot_rows, slot_to_role, eligibility, people_norm, p1_roles_by_person = main_pass_schedule(
        long_df, availability, service_dates, role_codes, all_role_headers=role_cols,
        exclusions_tuple=(is_excluded, excl_summary, brooklyn_excluded_names)
    )

    # Tables
    schedule_df = build_schedule_df(schedule_cells, slot_rows, service_dates, name_display_map)

    total_slots = schedule_df.size
    filled_slots = int((schedule_df != "").sum().sum())
    fill_rate = (filled_slots / total_slots) if total_slots else 0.0
    unfilled = total_slots - filled_slots

    # Per-person summary + details
    per_series = pd.Series(assign_count_norm, name="Assignments")
    per_series.index = [name_display_map.get(k, k) for k in per_series.index]
    per_person = (
        per_series.sort_values(ascending=False)
        .reset_index()
        .rename(columns={"index": "Person"})
    )
    details_lookup = build_person_assignment_details(schedule_cells, name_display_map)
    per_person["Locations & Dates"] = per_person["Person"].map(lambda nm: details_lookup.get(nm, ""))

    few_yes_display = [name_display_map.get(k, k) for k in few_yes_list_norm]

    served_p1_people_norm = served_in_priority_one(schedule_cells, p1_roles_by_person, slot_to_role)
    p1_people_norm = sorted([p for p in people_norm if p1_roles_by_person.get(p)])
    unmet_p1_norm = [p for p in p1_people_norm if p not in served_p1_people_norm]
    unmet_p1_display = [name_display_map.get(k, k) for k in unmet_p1_norm]

    # UPDATED: Unscheduled but Available list per date — sorted by campus order (then name)
    unscheduled_df = build_unscheduled_available(
        schedule_cells, service_dates, availability, name_display_map, code_map, campus_map
    )

    st.success(f"Schedule generated for **{sheet_name}**")
    st.write(f"Filled slots: **{filled_slots} / {total_slots}**  (Fill rate: **{fill_rate:.1%}**)  •  Unfilled: **{unfilled}**")
    if exclusions_file is not None:
        st.caption(f"Exclusions: {excl_summary}")

    st.subheader("Schedule (each slot is its own row)")
    st.dataframe(schedule_df, use_container_width=True)

    st.subheader("Assignment Summary")
    st.dataframe(per_person, use_container_width=True)

    st.subheader("People with < 2 'Yes' dates (info)")
    st.dataframe(pd.DataFrame({"Person": few_yes_display}), use_container_width=True)

    if unmet_p1_display:
        st.subheader("Unmet Priority-1 (info)")
        st.caption("These volunteers have at least one Priority-1 location but weren’t scheduled into any Priority-1 slot (capacity/availability constraints).")
        st.dataframe(pd.DataFrame({"Person": unmet_p1_display}), use_container_width=True)

    st.subheader("Unscheduled but Available — by Date (Name + Code)")
    st.caption("Sorted by campus: Pretoria → Nelspruit (Nel) → Polokwane (POL) → Tygerberg (TGB). Campus is inferred from the codes column (NEL/POL/TGB).")
    st.dataframe(unscheduled_df, use_container_width=True)

    # Excel export
    wb = Workbook()
    ws = wb.create_sheet(sheet_name) if wb.active.title == "Sheet" else wb.active
    ws.title = sheet_name

    header = ["Position / Slot"] + [d.strftime("%Y-%m-%d") for d in service_dates]
    ws.append(header)
    for row_name in slot_rows:
        row_vals = [row_name] + [
            ", ".join([name_display_map.get(nm, nm) for nm in schedule_cells[(row_name, d)]])
            for d in service_dates
        ]
        ws.append(row_vals)
    excel_autofit(ws)

    ws3 = wb.create_sheet("Assignment Summary")
    ws3.append(["Person", "Assignments", "Locations & Dates"])
    for _, r in per_person.iterrows():
        ws3.append([r["Person"], int(r["Assignments"]), r.get("Locations & Dates", "")])
    excel_autofit(ws3)

    if few_yes_display:
        ws2 = wb.create_sheet("Fewer than 2 Yes (info)")
        ws2.append(["Person"])
        for p in few_yes_display:
            ws2.append([p])
        excel_autofit(ws2)

    if unmet_p1_display:
        ws4 = wb.create_sheet("Unmet Priority-1 (info)")
        ws4.append(["Person"])
        for p in unmet_p1_display:
            ws4.append([p])
        excel_autofit(ws4)

    # Unscheduled by Date (sorted by campus)
    ws5 = wb.create_sheet("Unscheduled by Date")
    header_pairs = []
    for d in service_dates:
        ds = d.strftime("%Y-%m-%d")
        header_pairs.extend([ds, f"{ds} code"])
    ws5.append([" "] + header_pairs)

    for i in range(unscheduled_df.shape[0]):
        row_vals = [i + 1] + [unscheduled_df.iloc[i, j] for j in range(unscheduled_df.shape[1])]
        ws5.append(row_vals)
    excel_autofit(ws5)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    st.download_button(
        "Download Excel (.xlsx)",
        data=buf,
        file_name=f"uKids_schedule_{sheet_name.replace(' ','_')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Upload Positions + Responses (and optional Exclusions), then click **Generate Schedule**.")
