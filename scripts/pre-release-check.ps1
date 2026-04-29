param(
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"

Write-Host "[pre-release] checking version sync..."
./scripts/sync-versions.ps1 -Check

Write-Host "[pre-release] checking formatting..."
cargo fmt --all -- --check

if (-not $SkipTests) {
    Write-Host "[pre-release] running tests..."
    cargo test --workspace
} else {
    Write-Host "[pre-release] tests skipped by request."
}

Write-Host "[pre-release] all checks passed."
