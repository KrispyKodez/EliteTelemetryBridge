$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python)) {
    throw "The project virtual environment was not found at $python"
}

Set-Location -LiteralPath $projectRoot

$versionMatch = Select-String `
    -LiteralPath (Join-Path $projectRoot "app.py") `
    -Pattern '^VERSION = "([^"]+)"$'

if (-not $versionMatch) {
    throw "Unable to read VERSION from app.py"
}

$version = $versionMatch.Matches[0].Groups[1].Value
$releaseRoot = Join-Path $projectRoot "release"
$portableRoot = Join-Path $projectRoot "dist\EliteTelemetryBridge"
$sourceStage = Join-Path $releaseRoot "EliteTelemetryBridge-v$version-source"
$sourceZip = Join-Path $releaseRoot "EliteTelemetryBridge-v$version-source.zip"
$windowsZip = Join-Path $releaseRoot "EliteTelemetryBridge-v$version-windows-x64.zip"

function Assert-ProjectChild([string]$Path) {
    $root = [IO.Path]::GetFullPath($projectRoot).TrimEnd('\') + '\'
    $candidate = [IO.Path]::GetFullPath($Path)

    if (-not $candidate.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Refusing to modify a path outside the project: $candidate"
    }
}

foreach ($target in @($sourceStage, $sourceZip, $windowsZip)) {
    Assert-ProjectChild $target

    if (Test-Path -LiteralPath $target) {
        Remove-Item -LiteralPath $target -Recurse -Force
    }
}

New-Item -ItemType Directory -Force -Path $releaseRoot | Out-Null

& $python -m unittest discover -s tests -v

if ($LASTEXITCODE -ne 0) {
    throw "Tests failed; release build stopped."
}

& $python -m PyInstaller `
    --noconfirm `
    --clean `
    --onedir `
    --console `
    --name EliteTelemetryBridge `
    app.py

if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller failed."
}

Copy-Item `
    -LiteralPath (Join-Path $projectRoot "telemetry-settings.example.json") `
    -Destination (Join-Path $portableRoot "telemetry-settings.json")

foreach ($file in @(
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
    "THIRD-PARTY-NOTICES.md"
)) {
    Copy-Item `
        -LiteralPath (Join-Path $projectRoot $file) `
        -Destination $portableRoot
}

$licenseCollector = @'
import shutil
import sys
from importlib.metadata import distribution
from pathlib import Path

destination = Path(sys.argv[1]) / "licenses"
destination.mkdir(parents=True, exist_ok=True)

packages = (
    "aiohttp",
    "aiohappyeyeballs",
    "aiosignal",
    "attrs",
    "frozenlist",
    "multidict",
    "propcache",
    "yarl",
    "idna",
    "watchdog",
)

for package in packages:
    installed = distribution(package)
    package_directory = destination / f"{installed.metadata['Name']}-{installed.version}"
    package_directory.mkdir(parents=True, exist_ok=True)

    for entry in installed.files or ():
        if "license" not in str(entry).lower() and "notice" not in str(entry).lower():
            continue
        source = Path(installed.locate_file(entry))
        if source.is_file():
            shutil.copy2(source, package_directory / source.name)

python_license = Path(sys.base_prefix) / "LICENSE.txt"
if python_license.is_file():
    python_directory = destination / "Python"
    python_directory.mkdir(parents=True, exist_ok=True)
    shutil.copy2(python_license, python_directory / "LICENSE.txt")
'@

$licenseCollector | & $python - $portableRoot

New-Item -ItemType Directory -Force -Path $sourceStage | Out-Null

foreach ($file in @(
    ".gitignore",
    "app.py",
    "build-release.ps1",
    "CHANGELOG.md",
    "LICENSE",
    "README.md",
    "requirements.txt",
    "requirements-dev.txt",
    "telemetry-settings.example.json",
    "THIRD-PARTY-NOTICES.md"
)) {
    Copy-Item `
        -LiteralPath (Join-Path $projectRoot $file) `
        -Destination $sourceStage
}

Copy-Item `
    -LiteralPath (Join-Path $projectRoot "tests") `
    -Destination $sourceStage `
    -Recurse

Copy-Item `
    -LiteralPath (Join-Path $projectRoot ".github") `
    -Destination $sourceStage `
    -Recurse

Copy-Item `
    -LiteralPath (Join-Path $projectRoot "integrations") `
    -Destination $sourceStage `
    -Recurse

Copy-Item `
    -LiteralPath (Join-Path $projectRoot "integrations") `
    -Destination $portableRoot `
    -Recurse

# Copying test directories can carry local Python bytecode into the staged
# source tree. Purge generated caches before creating the public archive.
Get-ChildItem `
    -LiteralPath $sourceStage `
    -Directory `
    -Filter "__pycache__" `
    -Recurse `
    -Force |
    Remove-Item -Recurse -Force

Get-ChildItem `
    -LiteralPath $sourceStage `
    -File `
    -Recurse `
    -Force |
    Where-Object { $_.Extension -in @(".pyc", ".pyo") } |
    Remove-Item -Force

$zipBuilder = @'
import os
import sys
import time
import zipfile
from pathlib import Path


def build_zip(source_text: str, destination_text: str) -> None:
    source = Path(source_text).resolve()
    destination = Path(destination_text).resolve()
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    last_error = None

    for attempt in range(10):
        try:
            temporary.unlink(missing_ok=True)

            with zipfile.ZipFile(
                temporary,
                "w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=6,
            ) as archive:
                for path in sorted(source.rglob("*")):
                    if path.is_file():
                        archive.write(
                            path,
                            Path(source.name) / path.relative_to(source),
                        )

            with zipfile.ZipFile(temporary, "r") as archive:
                bad_member = archive.testzip()

                if bad_member is not None:
                    raise RuntimeError(f"Corrupt ZIP member: {bad_member}")

            os.replace(temporary, destination)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.25 * (attempt + 1))

    raise RuntimeError(
        f"Unable to create {destination} after retries: {last_error}"
    )


build_zip(sys.argv[1], sys.argv[2])
build_zip(sys.argv[3], sys.argv[4])
'@

$zipBuilder | & $python - $sourceStage $sourceZip $portableRoot $windowsZip

if ($LASTEXITCODE -ne 0) {
    throw "ZIP creation or validation failed."
}

Write-Host ""
Write-Host "Release complete:"
Write-Host "  $sourceZip"
Write-Host "  $windowsZip"
