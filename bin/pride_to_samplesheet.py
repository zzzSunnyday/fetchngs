#!/usr/bin/env python

"""
Generate pipeline-specific samplesheets from PRIDE project metadata.

Supports:
- Default/quantms: SDRF-format TSV
- mhcquant: 4-column TSV

If an existing SDRF was fetched, updates file paths to local paths.
Otherwise generates a minimal samplesheet from project metadata.
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger()


def parse_args(args=None):
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate pipeline-specific samplesheets from PRIDE project metadata.",
    )
    parser.add_argument("accession", type=str, help="ProteomeXchange accession.")
    parser.add_argument("metadata_json", type=Path, help="Project metadata JSON.")
    parser.add_argument("files_tsv", type=Path, help="File listing TSV.")
    parser.add_argument("output_dir", type=Path, help="Output directory.")
    parser.add_argument("--sdrf", type=Path, default=None, help="Existing SDRF file.")
    parser.add_argument(
        "--pipeline",
        type=str,
        default=None,
        choices=["quantms", "mhcquant"],
        help="Target nf-core pipeline for samplesheet format.",
    )
    parser.add_argument("--downloaded-dir", type=Path, default=None, help="Downloaded files directory.")
    parser.add_argument(
        "-l",
        "--log-level",
        default="WARNING",
        choices=("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"),
    )
    return parser.parse_args(args)


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


def load_files_tsv(files_tsv):
    """Load file listings from TSV."""
    with open(files_tsv, "r") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def get_raw_files(files):
    """Get RAW category files from file listing."""
    return [f for f in files if f.get("fileCategory", "").upper() == "RAW"]


def update_sdrf_paths(sdrf_path, accession, downloaded_dir, output_file):
    """Update an existing SDRF with local file paths and write to output."""
    with open(sdrf_path, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        headers = next(reader)
        rows = [dict(zip(headers, row)) for row in reader]

    if downloaded_dir:
        for row in rows:
            for col_name in list(row.keys()):
                if "comment[data file]" in col_name.lower() or "comment[associated file uri]" in col_name.lower():
                    original = row[col_name]
                    basename = Path(original).name if original else ""
                    if basename:
                        row[col_name] = str(downloaded_dir / accession / basename)

    with open(output_file, "w", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=headers, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def generate_quantms_samplesheet(accession, metadata, files, sdrf_path, downloaded_dir, output_dir):
    """Generate quantms-compatible SDRF samplesheet."""
    output_file = output_dir / f"{accession}.samplesheet.tsv"

    if sdrf_path and sdrf_path.is_file() and sdrf_path.stat().st_size > 0:
        update_sdrf_paths(sdrf_path, accession, downloaded_dir, output_file)
    else:
        raw_files = get_raw_files(files)
        organisms = flatten_list_field(metadata.get("organisms", ""))
        instruments = flatten_list_field(metadata.get("instruments", ""))
        diseases = flatten_list_field(metadata.get("diseases", ""))

        headers = [
            "source name",
            "characteristics[organism]",
            "characteristics[disease]",
            "characteristics[organism part]",
            "characteristics[cell type]",
            "comment[instrument]",
            "comment[label]",
            "comment[fraction identifier]",
            "comment[data file]",
            "comment[file uri]",
        ]

        with open(output_file, "w", newline="") as fout:
            writer = csv.writer(fout, delimiter="\t")
            writer.writerow(headers)
            for rf in raw_files:
                filename = rf.get("fileName", "")
                file_path = str(downloaded_dir / accession / filename) if downloaded_dir else filename
                sample_name = Path(filename).stem if filename else accession

                writer.writerow([
                    sample_name,
                    organisms,
                    diseases,
                    "",
                    "",
                    instruments,
                    "label free sample",
                    "1",
                    file_path,
                    rf.get("ftpLink", ""),
                ])

    logger.info(f"Wrote samplesheet to {output_file}")


def generate_mhcquant_samplesheet(accession, sdrf_path, downloaded_dir, output_dir):
    """Generate mhcquant-compatible samplesheet (4-column TSV) from SDRF.

    Requires an SDRF file. Uses 'factor value[*]' columns for the Sample
    column and 'comment[data file]' for ReplicateFileName.
    """
    if not sdrf_path or not sdrf_path.is_file() or sdrf_path.stat().st_size == 0:
        logger.error(
            f"mhcquant samplesheet requires an SDRF file for {accession}. "
            f"No SDRF available - cannot generate mhcquant samplesheet."
        )
        sys.exit(1)

    with open(sdrf_path, "r") as f:
        reader = csv.reader(f, delimiter="\t")
        headers = next(reader)
        rows = [dict(zip(headers, row)) for row in reader]

    # Find factor value and data file columns
    factor_cols = [h for h in headers if h.lower().startswith("factor value[")]
    data_file_col = next((h for h in headers if h.lower() == "comment[data file]"), None)

    if not data_file_col:
        logger.error(f"SDRF for {accession} has no 'comment[data file]' column.")
        sys.exit(1)

    output_file = output_dir / f"{accession}.samplesheet.tsv"
    with open(output_file, "w", newline="") as fout:
        writer = csv.writer(fout, delimiter="\t")
        writer.writerow(["ID", "Sample", "Condition", "ReplicateFileName"])
        for i, row in enumerate(rows, start=1):
            # Build Sample from factor value columns (concatenate if multiple)
            if factor_cols:
                sample_parts = [row.get(col, "").strip() for col in factor_cols if row.get(col, "").strip()]
                sample_name = "_".join(sample_parts) if sample_parts else f"sample_{i}"
            else:
                sample_name = f"sample_{i}"

            filename = row.get(data_file_col, "")
            file_path = str(downloaded_dir / accession / filename) if downloaded_dir and filename else filename
            writer.writerow([i, sample_name, "A", file_path])

    logger.info(f"Wrote mhcquant samplesheet to {output_file}")


def main(args=None):
    """Main entry point."""
    args = parse_args(args)
    logging.basicConfig(level=args.log_level, format="[%(levelname)s] %(message)s")

    if not args.metadata_json.is_file():
        logger.error(f"Metadata JSON not found: {args.metadata_json}")
        sys.exit(1)
    if not args.files_tsv.is_file():
        logger.error(f"Files TSV not found: {args.files_tsv}")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    with open(args.metadata_json, "r") as f:
        all_metadata = json.load(f)
    metadata = all_metadata.get(args.accession, {})
    files = load_files_tsv(args.files_tsv)
    sdrf_path = args.sdrf if args.sdrf and args.sdrf.is_file() else None

    if args.pipeline == "mhcquant":
        generate_mhcquant_samplesheet(
            args.accession, sdrf_path, args.downloaded_dir, args.output_dir
        )
    else:
        generate_quantms_samplesheet(
            args.accession, metadata, files, sdrf_path, args.downloaded_dir, args.output_dir
        )


if __name__ == "__main__":
    sys.exit(main())
