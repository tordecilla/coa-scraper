"""
generate_catalogs.py

Generates two machine-readable CSVs from reports.csv:
  data/catalog_nga.csv  — National Government Agencies
  data/catalog_lgu.csv  — Local Government Units

Columns requiring programmatic extraction are flagged in the
'extraction_notes' column rather than silently guessing.

Usage:
    py generate_catalogs.py
"""

import csv
import re
import zipfile
from pathlib import Path

BASE_DIR = Path(__file__).parent
REPORTS_CSV = BASE_DIR / "data" / "reports.csv"
NGA_CATALOG = BASE_DIR / "data" / "catalog_nga.csv"
LGU_CATALOG = BASE_DIR / "data" / "catalog_lgu.csv"

# ---------------------------------------------------------------------------
# Suffix extraction (shared by NGA agency name and LGU name)
# ---------------------------------------------------------------------------

# Order matters: more specific suffixes first.
_SUFFIX_PATTERNS = [
    r"Consolidted\s+Annual\s+Audit\s+Reports?",   # typo: "Consolidted"
    r"Consolidated\s+Annual\s+Audit\s+Reports?",
    r"Annual\s+Audit\s+Repots?",                  # typo: "Repot"
    r"Annual\s+Audit\s+Repors?\b",                # typo: "Repor"
    r"Annual\s+audit\s+Reports?",                 # case variant
    r"Annual\s+Audit\s+Reports?",
    r"Audit\s+Reports?",
    r"[A-Z]?Executive\s+Summ[a-z]*s?",            # covers NExecutive, Summry, etc.
    r"Management\s+Letters?",
    r"Audited\s+Financial\s+Statements?",
    r"Annual\s+Audit",                             # bare (e.g. "Annual Audit 2015")
]

_FULL_RE = re.compile(
    r"^(.+?)\s+(" + "|".join(_SUFFIX_PATTERNS) + r")\s+(\d{3,4}[A-Za-z]?)\s*$",
    re.IGNORECASE,
)
_NO_YEAR_RE = re.compile(
    r"^(.+?)\s+(" + "|".join(_SUFFIX_PATTERNS) + r")\s*$",
    re.IGNORECASE,
)


def extract_entity_name(title: str) -> tuple[str, list[str]]:
    """
    Strip the report-type suffix and year from a title to get the entity name.
    Returns (entity_name, notes_list).
    notes_list is empty when extraction is clean; contains warning strings otherwise.
    """
    notes = []

    m = _FULL_RE.match(title)
    if m:
        year_str = m.group(3)
        if len(year_str.rstrip("ABCDabcd")) < 4:
            notes.append(f"malformed year in title: '{year_str}'")
        return m.group(1).strip(), notes

    m = _NO_YEAR_RE.match(title)
    if m:
        notes.append("no year found in title")
        return m.group(1).strip(), notes

    notes.append("could not strip suffix from title — full title used as entity name")
    return title.strip(), notes


# ---------------------------------------------------------------------------
# ZIP contents
# ---------------------------------------------------------------------------

def zip_contents(path_str: str) -> tuple[str, list[str]]:
    """
    Return a pipe-separated list of files inside the ZIP.
    Returns (contents_string, notes_list).
    """
    notes = []
    p = BASE_DIR / path_str.replace("\\", "/").lstrip("./")
    if not p.exists():
        notes.append(f"ZIP not found on disk: {p}")
        return "", notes
    try:
        with zipfile.ZipFile(p) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            return " | ".join(sorted(names)), notes
    except zipfile.BadZipFile as e:
        notes.append(f"bad ZIP file: {e}")
        return "", notes
    except Exception as e:
        notes.append(f"ZIP read error: {e}")
        return "", notes


# ---------------------------------------------------------------------------
# LGU agency string parsing
# ---------------------------------------------------------------------------

_SUBGROUP_TYPE_MAP = {
    "provinces": "Province",
    "province": "Province",
    "cities": "City",
    "city": "City",
    "municipalities": "Municipality",
    "municipality": "Municipality",
}


def parse_lgu_agency(agency: str) -> tuple[str, str, str, list[str]]:
    """
    Parse the LGU 'agency' column into (region, province, lgu_type, notes).

    Province logic:
    - Subgroup = "Provinces" → province is the LGU itself; return sentinel
      DERIVE_FROM_NAME (caller replaces with lgu_name).
    - Subgroup = "Cities" / "Municipalities" → province unknown without
      external lookup; left blank and flagged.
    - No subgroup (regional-level entry) → province left blank.
    - Special / unrecognised subgroups → flagged.
    """
    notes = []

    # --- Special cases ---
    if agency.startswith("State Universities and Colleges"):
        # e.g. "State Universities and Colleges / Regional Satellite Audit Office - ..."
        return "State Universities and Colleges", "", "Special", [
            "non-geographic grouping; province not applicable"
        ]

    if agency.startswith("Regional Satellite Audit Office"):
        return "Regional Satellite Audit Office - Negros Island and Siquijor", "", "Special", [
            "non-geographic grouping; province not applicable"
        ]

    # --- Split on ' / ' to separate region from subgroup ---
    if " / " in agency:
        region_part, subgroup = agency.split(" / ", 1)
    else:
        region_part = agency
        subgroup = ""

    # NCR format: "National Capital Region (NCR) - Metropolitan Manila"
    # Strip the " - Sub-area" from the region part (not a true province in PH admin sense)
    if " - " in region_part:
        region, _ = region_part.split(" - ", 1)
        region = region.strip()
    else:
        region = region_part.strip()

    # --- Determine type and province from subgroup ---
    subgroup_key = subgroup.strip().lower()
    if subgroup_key in _SUBGROUP_TYPE_MAP:
        lgu_type = _SUBGROUP_TYPE_MAP[subgroup_key]
        if lgu_type == "Province":
            province = "DERIVE_FROM_NAME"
        else:
            province = ""
            is_ncr = "NCR" in region or "National Capital Region" in region
            if not is_ncr:
                notes.append(
                    f"province unknown for {lgu_type.lower()}-level LGU; needs geographic lookup"
                )
    elif subgroup_key == "":
        # NCR entities without a subgroup are city-owned institutions
        # (e.g. Pamantasan ng Lungsod ng Maynila, Quezon City General Hospital).
        # Other regions without a subgroup are genuine regional bodies.
        is_ncr = "NCR" in region or "National Capital Region" in region
        lgu_type = "City" if is_ncr else "Regional"
        province = ""
    else:
        lgu_type = "Special"
        province = ""
        notes.append(f"unrecognised subgroup '{subgroup}'; type set to Special")

    return region, province, lgu_type, notes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_reports() -> list[dict]:
    with open(REPORTS_CSV, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def build_nga_catalog(rows: list[dict]) -> list[dict]:
    records = []
    nga_rows = [r for r in rows if r["category"] == "NGA" and r["status"] == "downloaded"]
    total = len(nga_rows)
    print(f"[NGA] Processing {total} downloaded rows...")

    for i, row in enumerate(nga_rows, 1):
        if i % 200 == 0:
            print(f"  {i}/{total}")

        notes = []
        title = row["title"]
        file_path = row["file_path"]
        ext = Path(file_path.replace("\\", "/")).suffix.lower().lstrip(".")

        agency_name, entity_notes = extract_entity_name(title)
        notes.extend(entity_notes)

        if ext == "zip":
            contents, zip_notes = zip_contents(file_path)
            notes.extend(zip_notes)
        else:
            contents = ""

        records.append({
            "Year": row["year"],
            "Main Agency": row["agency"],
            "Agency": agency_name,
            "Description": title,
            "File Type": ext,
            "Filename": Path(file_path.replace("\\", "/")).name,
            "Path": file_path,
            "Contents": contents,
            "Source URL": row["url"],
            "Extraction Notes": "; ".join(notes),
        })

    return records


def build_lgu_catalog(rows: list[dict]) -> list[dict]:
    records = []
    lgu_rows = [r for r in rows if r["category"] == "LGU" and r["status"] == "downloaded"]
    total = len(lgu_rows)
    print(f"[LGU] Processing {total} downloaded rows...")

    for i, row in enumerate(lgu_rows, 1):
        if i % 200 == 0:
            print(f"  {i}/{total}")

        notes = []
        title = row["title"]
        file_path = row["file_path"]
        ext = Path(file_path.replace("\\", "/")).suffix.lower().lstrip(".")

        lgu_name, entity_notes = extract_entity_name(title)
        notes.extend(entity_notes)

        region, province, lgu_type, agency_notes = parse_lgu_agency(row["agency"])
        notes.extend(agency_notes)

        # Province for province-level LGUs is the LGU itself
        if province == "DERIVE_FROM_NAME":
            province = lgu_name if "could not strip" not in " ".join(notes) else ""

        if ext == "zip":
            contents, zip_notes = zip_contents(file_path)
            notes.extend(zip_notes)
        else:
            contents = ""

        records.append({
            "Year": row["year"],
            "Region": region,
            "Province": province,
            "LGU Name": lgu_name,
            "LGU Type": lgu_type,
            "Description": title,
            "File Type": ext,
            "Filename": Path(file_path.replace("\\", "/")).name,
            "Path": file_path,
            "Contents": contents,
            "Source URL": row["url"],
            "Extraction Notes": "; ".join(notes),
        })

    return records


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    # utf-8-sig writes a BOM so Excel auto-detects UTF-8 (avoids ñ → Ã± corruption)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Written: {path} ({len(rows)} rows)")


def summarise_flags(rows: list[dict], label: str) -> None:
    flagged = [r for r in rows if r["Extraction Notes"]]
    print(f"\n[{label}] {len(flagged)}/{len(rows)} rows have extraction notes.")
    # Show unique note types
    note_types: dict[str, int] = {}
    for r in flagged:
        for note in r["Extraction Notes"].split("; "):
            note = note.strip()
            if note:
                # Normalise to a short key
                key = re.sub(r"'[^']*'", "<val>", note)
                note_types[key] = note_types.get(key, 0) + 1
    for note, count in sorted(note_types.items(), key=lambda x: -x[1]):
        print(f"  {count:4d}x  {note}")


def main():
    print(f"Loading {REPORTS_CSV}...")
    rows = load_reports()
    print(f"Total rows: {len(rows)}")

    nga_records = build_nga_catalog(rows)
    lgu_records = build_lgu_catalog(rows)

    NGA_FIELDS = [
        "Year", "Main Agency", "Agency", "Description",
        "File Type", "Filename", "Path", "Contents", "Source URL", "Extraction Notes",
    ]
    LGU_FIELDS = [
        "Year", "Region", "Province", "LGU Name", "LGU Type", "Description",
        "File Type", "Filename", "Path", "Contents", "Source URL", "Extraction Notes",
    ]

    write_csv(NGA_CATALOG, nga_records, NGA_FIELDS)
    write_csv(LGU_CATALOG, lgu_records, LGU_FIELDS)

    summarise_flags(nga_records, "NGA")
    summarise_flags(lgu_records, "LGU")


if __name__ == "__main__":
    main()
