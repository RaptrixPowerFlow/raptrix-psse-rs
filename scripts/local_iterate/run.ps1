<#
.SYNOPSIS
    Local iterate loop for RPF generation quality work.

.DESCRIPTION
    Regenerates *_static.rpf files from tests/data/external/ via raptrix-psse-rs,
    runs raptrix-core's pv_pq_harness against the RAW + RPF pair, and produces
    a compact RAW vs RPF delta report. Designed to live entirely under
    scripts/local_iterate/ which is gitignored.

    Default flow (no args):
      1. cargo build --release -p raptrix-psse-rs (in repo root).
      2. Convert each RAW under tests/data/external/ -> out/rpf/<name>_static.rpf.
      3. Run python_tests/regression/pv_pq_harness.py from raptrix-core.
      4. Parse the JSONL log + converter / importer stderr.
      5. Print a fixed-width delta table and write out/delta_report.md.

.PARAMETER Cases
    Optional comma-separated list of case stems (without extension) to limit
    the run, e.g. -Cases ACTIVSg25k,Base_Eastern_Interconnect_515GW.

.PARAMETER Mode
    Comma-separated subset of pv,pq passed through to the harness. Default
    is "pv,pq".

.PARAMETER SkipBuild
    Skip cargo build (useful when iterating on the report formatter only).

.PARAMETER SkipConvert
    Skip RAW -> RPF regeneration. Reuses out/rpf/.

.PARAMETER SkipHarness
    Skip the harness run and only re-format the existing JSONL log.

.PARAMETER LabelTag
    Optional tag stamped into the report header (e.g. "before-fix-A").

.NOTES
    All output paths are relative to repo root (psse-rs). The script auto-detects
    the sibling raptrix-core checkout via "..\raptrix-core".
#>
[CmdletBinding()]
param(
    [string]$Cases = "",
    [string]$Mode = "pv,pq",
    [switch]$SkipBuild,
    [switch]$SkipConvert,
    [switch]$SkipHarness,
    [string]$LabelTag = ""
)

$ErrorActionPreference = "Stop"

# Resolve repo roots relative to this script.
$ScriptDir = Split-Path -Parent $PSCommandPath
$LoopRoot = $ScriptDir
$PsseRoot = Resolve-Path (Join-Path $ScriptDir "..\..") | Select-Object -ExpandProperty Path
$CoreRoot = Resolve-Path (Join-Path $PsseRoot "..\raptrix-core") -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Path
if (-not $CoreRoot) {
    throw "raptrix-core checkout not found at $PsseRoot\..\raptrix-core. Adjust path before running."
}

$ExternalRawDir = Join-Path $PsseRoot "tests\data\external"
if (-not (Test-Path $ExternalRawDir)) {
    throw "External RAW dir not found: $ExternalRawDir"
}

# Cases filter applies to BOTH the converter and the harness. To keep harness
# input scoped, we stage a curated raw_subset/ dir with file-system copies of
# just the requested RAWs, then point the harness at that dir. When -Cases is
# empty we use the full external dir directly.
$WantedStems = @()
if ($Cases.Trim().Length -gt 0) {
    $WantedStems = $Cases -split "," | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ }
}

# Use a non-OneDrive target dir to dodge Windows Application Control / Defender
# binary scans on freshly compiled cargo test binaries. (OneDrive sync paths
# trigger SmartScreen for unsigned EXEs in some Windows policies.)
if (-not $env:CARGO_TARGET_DIR) {
    $env:CARGO_TARGET_DIR = "C:\temp\raptrix-psse-rs-target"
}
New-Item -ItemType Directory -Force -Path $env:CARGO_TARGET_DIR | Out-Null

$OutDir = Join-Path $LoopRoot "out"
$OutRpfDir = Join-Path $OutDir "rpf"
$OutLogsDir = Join-Path $OutDir "logs"
foreach ($d in @($OutDir, $OutRpfDir, $OutLogsDir)) {
    New-Item -ItemType Directory -Force -Path $d | Out-Null
}

$ConvertStderrLog = Join-Path $OutLogsDir "convert_stderr.log"
$HarnessStderrLog = Join-Path $OutLogsDir "harness_stderr.log"
$RegressionLog = Join-Path $OutLogsDir "solver_regression.log"
$DeltaReport = Join-Path $OutDir "delta_report.md"

# Effective RAW dir for the harness: full external dir, or a curated subset
# under out/raw_subset when -Cases is non-empty.
$RawSubsetDir = Join-Path $OutDir "raw_subset"
if ($WantedStems.Count -gt 0) {
    Remove-Item -Recurse -Force $RawSubsetDir -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path $RawSubsetDir | Out-Null
    $allRaws = Get-ChildItem $ExternalRawDir -File | Where-Object { $_.Extension -ieq ".raw" }
    foreach ($raw in $allRaws) {
        $stem = [System.IO.Path]::GetFileNameWithoutExtension($raw.Name)
        if ($WantedStems -contains $stem.ToLower()) {
            Copy-Item -Force $raw.FullName -Destination (Join-Path $RawSubsetDir $raw.Name)
        }
    }
    $HarnessRawDir = $RawSubsetDir
    Write-Host "[iterate] cases filter: $($WantedStems -join ',') -> $RawSubsetDir" -ForegroundColor Cyan
} else {
    $HarnessRawDir = $ExternalRawDir
}

# ---------------------------------------------------------------------------
# Step 1: cargo build
# ---------------------------------------------------------------------------
if (-not $SkipBuild) {
    Write-Host "[iterate] cargo build --release (target=$env:CARGO_TARGET_DIR)" -ForegroundColor Cyan
    Push-Location $PsseRoot
    try {
        & cargo build --release | Out-Host
        if ($LASTEXITCODE -ne 0) { throw "cargo build failed (exit=$LASTEXITCODE)" }
    } finally {
        Pop-Location
    }
}

$PsseExe = Join-Path $env:CARGO_TARGET_DIR "release\raptrix-psse-rs.exe"
if (-not (Test-Path $PsseExe)) {
    throw "raptrix-psse-rs.exe not found at $PsseExe (rebuild required)"
}

# ---------------------------------------------------------------------------
# Step 2: regenerate static RPFs from RAW files in tests/data/external/
# ---------------------------------------------------------------------------
function Should-Process-Stem {
    param([string]$Stem)
    if ($WantedStems.Count -eq 0) { return $true }
    return $WantedStems -contains $Stem.ToLower()
}

# Stems for which we don't generate an RPF (e.g., partial/dyn-only fixtures).
$ExcludeStems = @(
    # Texas2k_series24_case6 has a dyn fixture but the static RAW is exercised by
    # other tests; keep the iterate loop's RAW set aligned with the harness's
    # 14-RPF/15-RAW expectation so missing pairs don't pollute the delta report.
)

if (-not $SkipConvert) {
    Remove-Item $ConvertStderrLog -Force -ErrorAction SilentlyContinue
    Remove-Item -Recurse -Force (Join-Path $OutRpfDir "*.rpf") -ErrorAction SilentlyContinue

    $rawFiles = Get-ChildItem $ExternalRawDir -File | Where-Object {
        $_.Extension -ieq ".raw"
    }

    Write-Host "[iterate] regenerating RPFs from $($rawFiles.Count) RAW file(s) -> $OutRpfDir" -ForegroundColor Cyan
    foreach ($raw in $rawFiles) {
        $stem = [System.IO.Path]::GetFileNameWithoutExtension($raw.Name)
        if ($ExcludeStems -contains $stem) { continue }
        if (-not (Should-Process-Stem $stem)) { continue }
        $rpfOut = Join-Path $OutRpfDir ($stem + "_static.rpf")
        Write-Host "  - $($raw.Name) -> $($rpfOut | Split-Path -Leaf)"
        # Append converter stderr per file so importer-grade warnings can be
        # tallied in the report. PowerShell treats native-app stderr as
        # RemoteException records when ErrorActionPreference = Stop, so we
        # locally relax it around the call and route stderr through cmd /c.
        $rawPath = $raw.FullName
        $stderrTmp = New-TemporaryFile
        $prevPref = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            $cmdLine = "`"$PsseExe`" convert --raw `"$rawPath`" --output `"$rpfOut`" 2> `"$($stderrTmp.FullName)`""
            cmd /c $cmdLine | Out-Null
            $rc = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $prevPref
        }
        Add-Content -Path $ConvertStderrLog -Value "===== $($raw.Name) (exit=$rc) ====="
        Add-Content -Path $ConvertStderrLog -Value (Get-Content $stderrTmp.FullName -Raw)
        Remove-Item $stderrTmp.FullName -Force -ErrorAction SilentlyContinue
        if ($rc -ne 0) {
            Write-Host "    ! converter exit=$rc (continuing)" -ForegroundColor Yellow
        }
    }
}

# ---------------------------------------------------------------------------
# Step 3: run pv_pq_harness against RAW dir + RPF dir
# ---------------------------------------------------------------------------
if (-not $SkipHarness) {
    $venvPython = Join-Path $CoreRoot ".venv\Scripts\python.exe"
    if (-not (Test-Path $venvPython)) {
        throw "raptrix-core venv python not found at $venvPython. Activate the venv or build pyd first."
    }

    Write-Host "[iterate] running pv_pq_harness (modes=$Mode)" -ForegroundColor Cyan
    Remove-Item $RegressionLog -Force -ErrorAction SilentlyContinue
    Remove-Item $HarnessStderrLog -Force -ErrorAction SilentlyContinue

    Push-Location $CoreRoot
    try {
        # The robust_harness wrapper imports `python_tests._raptrix_env` to
        # resolve the local build path, which requires the core repo root on
        # sys.path. Setting PYTHONPATH is the lowest-friction way to make that
        # work via cmd /c. Unlike the upstream pv_pq_harness, robust_harness
        # catches per-case parse/solve exceptions so a single bad RPF (e.g.
        # disconnected-island topology errors that block parse_rpf) does not
        # bail out the whole run.
        $prevPyPath = $env:PYTHONPATH
        $env:PYTHONPATH = $CoreRoot
        $robustHarness = Join-Path $LoopRoot "robust_harness.py"
        $harnessQuoted = "`"$venvPython`" `"$robustHarness`" " +
            "--raw-dir `"$HarnessRawDir`" --rpf-dir `"$OutRpfDir`" --modes $Mode " +
            "--log `"$RegressionLog`" --label `"$LabelTag`" 2> `"$HarnessStderrLog`""
        $prevPref = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            cmd /c $harnessQuoted | Out-Host
            $hrc = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $prevPref
            $env:PYTHONPATH = $prevPyPath
        }
        if ($hrc -ne 0) {
            Write-Host "[iterate] robust_harness exit=$hrc (delta report may be partial)" -ForegroundColor Yellow
        }
    } finally {
        Pop-Location
    }
}

# ---------------------------------------------------------------------------
# Step 4: parse JSONL + stderr, write delta_report.md, print summary
# ---------------------------------------------------------------------------
$venvPython = Join-Path $CoreRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    Write-Host "[iterate] note: $venvPython not found; falling back to system python" -ForegroundColor Yellow
    $venvPython = "python"
}
$parseScript = Join-Path $LoopRoot "parse_log.py"
$parseArgs = @(
    $parseScript,
    "--regression-log", $RegressionLog,
    "--converter-stderr", $ConvertStderrLog,
    "--harness-stderr", $HarnessStderrLog,
    "--output", $DeltaReport
)
if ($LabelTag.Length -gt 0) {
    $parseArgs += @("--label", $LabelTag)
}

Write-Host "[iterate] formatting delta report -> $DeltaReport" -ForegroundColor Cyan
& $venvPython @parseArgs | Out-Host
if ($LASTEXITCODE -ne 0) {
    throw "parse_log.py failed (exit=$LASTEXITCODE)"
}

Write-Host ""
Write-Host "[iterate] done. Report: $DeltaReport" -ForegroundColor Green
