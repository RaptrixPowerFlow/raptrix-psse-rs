#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <input_report.txt> <output.csv>" >&2
  exit 1
fi

input_report="$1"
output_csv="$2"

awk '
BEGIN {
  print "output_file,timing_ms,total_rows,all_canonical"
}
/^\[timing\]/ {
  timing=$4
  getline  # [view]
  getline  # RPF file: ...
  out=$3
  getline  # tables line
  rows=$5
  canon=$8
  gsub(",", "", rows)
  print out "," timing "," rows "," canon
}
' "$input_report" > "$output_csv"

echo "Wrote $output_csv"
