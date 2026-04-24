param(
    [switch]$ExternalGolden
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Convert-ToWslPath {
    param([Parameter(Mandatory = $true)][string]$WindowsPath)
    $resolved = (Resolve-Path $WindowsPath).Path
    if ($resolved -notmatch '^([A-Za-z]):\\') {
        throw "Unsupported Windows path format: $resolved"
    }
    $drive = $matches[1].ToLowerInvariant()
    $suffix = $resolved.Substring(2).Replace('\', '/')
    return "/mnt/$drive$suffix"
}

Write-Host "[verify] cargo fmt --check"
cargo fmt --all -- --check

Write-Host "[verify] cargo clippy"
cargo clippy --workspace --all-targets --all-features

Write-Host "[verify] cargo test --workspace --release"
cargo test --workspace --release

if ($ExternalGolden) {
    Write-Host "[verify] cargo build --release (CLI for golden script)"
    cargo build --release
    $repoWsl = Convert-ToWslPath -WindowsPath (Get-Location).Path
    Write-Host "[verify] external golden corpus via WSL: $repoWsl"
    & wsl.exe bash -lc "cd '$repoWsl' && ./scripts/verify-external-golden.sh"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "[verify] OK"
