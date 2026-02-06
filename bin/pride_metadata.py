#!/usr/bin/env python

"""
Query PRIDE REST API for project metadata and file listings.

Accepts a PXD (PRIDE) or MSV (MassIVE) accession and fetches:
1. Project-level metadata (title, organisms, instruments, etc.)
2. File listings (names, categories, sizes, checksums, download URLs)

Outputs metadata as TSV + JSON and file listings as TSV.
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen, Request

logger = logging.getLogger()

PRIDE_ID_REGEX = re.compile(r"^PXD[0-9]{6,}$")
MASSIVE_ID_REGEX = re.compile(r"^MSV[0-9]{9}$")
PRIDE_API_BASE = "https://www.ebi.ac.uk/pride/ws/archive/v2"

METADATA_FIELDS = (
    "accession",
    "title",
    "projectDescription",
    "sampleProcessingProtocol",
    "dataProcessingProtocol",
    "submissionType",
    "submissionDate",
    "publicationDate",
    "organisms",
    "organismParts",
    "diseases",
    "instruments",
    "quantificationMethods",
    "softwares",
    "projectTags",
    "keywords",
    "experimentTypes",
    "identifiedPTMStrings",
)

FILE_FIELDS = (
    "accession",
    "fileName",
    "fileCategory",
    "fileSizeBytes",
    "checksum",
    "checksumType",
    "ftpLink",
    "asperaLink",
)

PAGE_SIZE = 100


def parse_args(args=None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch project metadata and file listings from PRIDE Archive.",
    )
    parser.add_argument("accession", type=str, help="ProteomeXchange accession (PXD or MSV).")
    parser.add_argument("metadata_tsv", type=Path, help="Output metadata TSV file.")
    parser.add_argument("metadata_json", type=Path, help="Output metadata JSON file.")
    parser.add_argument("files_tsv", type=Path, help="Output file listing TSV.")
    parser.add_argument(
        "--file-types",
        type=str,
        default=None,
        help="Comma-separated file categories to include (e.g. RAW,PEAK,RESULT).",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        default="WARNING",
        choices=("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"),
    )
    return parser.parse_args(args)


def is_valid_identifier(identifier):
    """Check if the identifier is a valid ProteomeXchange/PRIDE/MassIVE identifier."""
    return bool(PRIDE_ID_REGEX.match(identifier) or MASSIVE_ID_REGEX.match(identifier))


def fetch_url(url, max_retries=3, initial_delay=5):
    """Fetch URL with retry logic for rate limiting and server errors."""
    delay = initial_delay
    request = Request(url, headers={"Accept": "application/json"})

    for attempt in range(max_retries + 1):
        try:
            with urlopen(request, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 404:
                logger.error(f"Resource not found: {url}")
                return None
            elif e.code == 429:
                retry_after = int(e.headers.get("Retry-After", delay))
                logger.warning(f"Rate limited. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                delay *= 2
            elif e.code >= 500:
                if attempt < max_retries:
                    logger.warning(f"Server error {e.code}. Retrying in {delay}s...")
                    time.sleep(delay)
                    delay *= 2
                else:
                    logger.error(f"Server error {e.code} after {max_retries} retries.")
                    sys.exit(1)
            else:
                logger.error(f"HTTP error {e.code}: {e.reason}")
                sys.exit(1)
        except URLError as e:
            if attempt < max_retries:
                logger.warning(f"Connection error: {e.reason}. Retrying in {delay}s...")
                time.sleep(delay)
                delay *= 2
            else:
                logger.error(f"Failed to connect after {max_retries} retries: {e.reason}")
                sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            sys.exit(1)

    return None


def flatten_list_field(value):
    """Flatten a list/dict field to a semicolon-separated string."""
    if value is None:
        return ""
    if isinstance(value, list):
        if value and isinstance(value[0], dict):
            return ";".join(str(item.get("name", item)) for item in value)
        return ";".join(str(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("name", value))
    return str(value)


def fetch_project_metadata(accession):
    """Fetch project metadata from PRIDE API."""
    url = f"{PRIDE_API_BASE}/projects/{accession}"
    logger.info(f"Fetching metadata for {accession}")
    return fetch_url(url)


def fetch_project_files(accession, file_types=None):
    """Fetch all files for a project from PRIDE API with pagination."""
    all_files = []
    page = 0

    while True:
        params = {"pageSize": PAGE_SIZE, "page": page}
        url = f"{PRIDE_API_BASE}/projects/{accession}/files?{urlencode(params)}"
        logger.info(f"Fetching files page {page} for {accession}")

        data = fetch_url(url)
        if data is None or len(data) == 0:
            break

        for entry in data:
            category = entry.get("fileCategory", {})
            cat_value = category.get("value", "") if isinstance(category, dict) else str(category)

            if file_types and cat_value.upper() not in file_types:
                continue

            locations = entry.get("publicFileLocations", [])
            ftp_link = aspera_link = ""
            for loc in locations:
                if loc.get("name") == "FTP Protocol":
                    ftp_link = loc.get("value", "")
                elif loc.get("name") == "Aspera Protocol":
                    aspera_link = loc.get("value", "")

            all_files.append({
                "accession": accession,
                "fileName": entry.get("fileName", ""),
                "fileCategory": cat_value,
                "fileSizeBytes": entry.get("fileSizeBytes", 0),
                "checksum": entry.get("checksum", ""),
                "checksumType": entry.get("checksumType", ""),
                "ftpLink": ftp_link,
                "asperaLink": aspera_link,
            })

        if len(data) < PAGE_SIZE:
            break
        page += 1

    return all_files


def main(args=None):
    """Main entry point."""
    args = parse_args(args)
    logging.basicConfig(level=args.log_level, format="[%(levelname)s] %(message)s")

    if not is_valid_identifier(args.accession):
        logger.error(
            f"Invalid ProteomeXchange identifier: '{args.accession}'. "
            f"Expected format: PXD000000 or MSV000000000"
        )
        sys.exit(1)

    for p in [args.metadata_tsv, args.metadata_json, args.files_tsv]:
        p.parent.mkdir(parents=True, exist_ok=True)

    # Fetch project metadata
    project_data = fetch_project_metadata(args.accession)
    if project_data is None:
        logger.error(f"No data found for: {args.accession}")
        sys.exit(1)

    # Write metadata TSV
    metadata = {f: flatten_list_field(project_data.get(f)) for f in METADATA_FIELDS}
    with open(args.metadata_tsv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=METADATA_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerow(metadata)
    logger.info(f"Wrote metadata for {args.accession} to {args.metadata_tsv}")

    # Write metadata JSON
    with open(args.metadata_json, "w") as f:
        json.dump({args.accession: project_data}, f, indent=2)
    logger.info(f"Wrote raw API response to {args.metadata_json}")

    # Fetch and write file listings
    file_types = None
    if args.file_types:
        file_types = [ft.strip().upper() for ft in args.file_types.split(",")]

    files = fetch_project_files(args.accession, file_types)
    if not files:
        logger.error(f"No files found for {args.accession}")
        sys.exit(1)

    with open(args.files_tsv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FILE_FIELDS, delimiter="\t")
        writer.writeheader()
        writer.writerows(files)
    logger.info(f"Wrote {len(files)} file(s) for {args.accession} to {args.files_tsv}")


if __name__ == "__main__":
    sys.exit(main())
