#!/usr/bin/env python3
"""
PowerUp Infinite - Data Append Utility
Appends new customer data to the master CSV files.
For each CSV, removes existing rows for any PF_ID in the new file, then appends.

Usage:
    python append_data.py                  # interactive: prompts for new-data folder
    python append_data.py C:\path\to\new   # pass folder as argument

The new-data folder should contain files with matching names (or recognised aliases):
    PF_level.csv
    Riskgroup_level.csv
    Scheme_level.csv
    Lines.csv
    Results.csv
    Invested_Value_Line.csv
"""

import os
import sys
import shutil
import glob
from datetime import datetime

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Master files and their PF_ID column name
MASTER_FILES = {
    'PF_level.csv':             'PF_ID',
    'Riskgroup_level.csv':      'PF_ID',
    'Scheme_level.csv':         'PF_ID',
    'Lines.csv':                'PF_ID',
    'Results.csv':              'PF_ID',
    'Invested_Value_Line.csv':  'PF_ID',
}

# Aliases: possible names a teammate might use → canonical master filename
FILE_ALIASES = {
    'pf_level.csv':                     'PF_level.csv',
    'pf level.csv':                     'PF_level.csv',
    'riskgroup_level.csv':              'Riskgroup_level.csv',
    'riskgroup level.csv':              'Riskgroup_level.csv',
    'risk_group_level.csv':             'Riskgroup_level.csv',
    'scheme_level.csv':                 'Scheme_level.csv',
    'scheme level.csv':                 'Scheme_level.csv',
    'lines.csv':                        'Lines.csv',
    'results.csv':                      'Results.csv',
    'invested_value_line.csv':          'Invested_Value_Line.csv',
    'invested value line.csv':          'Invested_Value_Line.csv',
    'invested_value.csv':               'Invested_Value_Line.csv',
}


def _backup(master_path):
    """Create a timestamped backup of the master file before modifying it."""
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = os.path.join(SCRIPT_DIR, '_backups')
    os.makedirs(backup_dir, exist_ok=True)
    base = os.path.basename(master_path)
    dst  = os.path.join(backup_dir, f'{os.path.splitext(base)[0]}_{ts}.csv')
    shutil.copy2(master_path, dst)
    return dst


def merge_file(master_path, new_path, id_col):
    """
    Remove rows in master where id_col matches any value in new_path,
    then append new_path rows.
    Returns (rows_removed, rows_added).
    """
    master = pd.read_csv(master_path, low_memory=False)
    new    = pd.read_csv(new_path,    low_memory=False)

    if id_col not in new.columns:
        print(f"    WARNING: '{id_col}' not found in {os.path.basename(new_path)} — skipping")
        return 0, 0

    new_ids      = set(new[id_col].astype(str).unique())
    mask_remove  = master[id_col].astype(str).isin(new_ids)
    rows_removed = mask_remove.sum()

    master_clean = master[~mask_remove]
    merged       = pd.concat([master_clean, new], ignore_index=True)

    # Write back with same encoding
    merged.to_csv(master_path, index=False, encoding='utf-8-sig')
    return int(rows_removed), len(new)


def main():
    if len(sys.argv) >= 2:
        new_folder = sys.argv[1].strip()
    else:
        print("PowerUp Infinite - Data Append Utility")
        print("=" * 40)
        new_folder = input("Path to folder containing new data files: ").strip().strip('"')

    if not os.path.isdir(new_folder):
        print(f"ERROR: folder not found: {new_folder}")
        sys.exit(1)

    # Discover CSV files in the new-data folder
    csv_files = glob.glob(os.path.join(new_folder, '*.csv'))
    if not csv_files:
        print(f"ERROR: no .csv files found in {new_folder}")
        sys.exit(1)

    print(f"\nNew data folder : {new_folder}")
    print(f"Files found     : {len(csv_files)}")
    print()

    matched = []
    for new_path in sorted(csv_files):
        base  = os.path.basename(new_path).lower()
        canon = FILE_ALIASES.get(base) or (os.path.basename(new_path) if os.path.basename(new_path) in MASTER_FILES else None)
        if canon is None:
            print(f"  SKIP (unrecognised): {os.path.basename(new_path)}")
            continue
        master_path = os.path.join(SCRIPT_DIR, canon)
        if not os.path.exists(master_path):
            print(f"  SKIP (master not found): {canon}")
            continue
        matched.append((new_path, master_path, MASTER_FILES[canon]))

    if not matched:
        print("No matching files found — nothing to do.")
        sys.exit(0)

    print(f"Files to merge: {len(matched)}")
    for new_path, master_path, _ in matched:
        print(f"  {os.path.basename(new_path)} -> {os.path.basename(master_path)}")

    print()
    confirm = input("Proceed? [y/N] ").strip().lower()
    if confirm != 'y':
        print("Aborted.")
        sys.exit(0)

    print()
    total_removed = 0
    total_added   = 0
    for new_path, master_path, id_col in matched:
        print(f"  Merging {os.path.basename(new_path)} ...")
        backup_path = _backup(master_path)
        print(f"    Backup -> {os.path.relpath(backup_path, SCRIPT_DIR)}")
        removed, added = merge_file(master_path, new_path, id_col)
        print(f"    Removed {removed} old rows, added {added} new rows")
        total_removed += removed
        total_added   += added

    print()
    print(f"Done. Total: {total_removed} rows replaced, {total_added} rows added.")
    print(f"Backups saved in: {os.path.join(SCRIPT_DIR, '_backups')}")


if __name__ == '__main__':
    main()
