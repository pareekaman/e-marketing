$ErrorActionPreference = 'Continue'
$base = 'C:\Program Files\MySQL\MySQL Server 8.4'
$dataDir = 'C:\ProgramData\MySQL\MySQL Server 8.4\Data'
$cfgDir = 'C:\ProgramData\MySQL\MySQL Server 8.4'
$cfg = "$cfgDir\my.ini"
$log = "$env:TEMP\mysql-setup.log"

function Log($m) {
    $line = "[{0}] {1}" -f (Get-Date -Format 'HH:mm:ss'), $m
    Add-Content -Path $log -Value $line
    Write-Host $line
}

"=== MySQL setup $(Get-Date) ===" | Set-Content -Path $log

# Stop and remove existing service if present (for clean re-init)
$svc = Get-Service -Name MySQL84 -ErrorAction SilentlyContinue
if ($svc) {
    if ($svc.Status -eq 'Running') {
        Log "Stopping MySQL84 service"
        try { Stop-Service MySQL84 -Force -ErrorAction Stop } catch { Log "Stop failed (will continue): $_" }
    }
    Log "Removing MySQL84 service"
    & "$base\bin\mysqld.exe" --remove MySQL84 | Out-Null
    Start-Sleep -Seconds 2
}

# Clean stale data dir
if (Test-Path $dataDir) {
    Log "Removing stale data dir"
    Remove-Item -Path $dataDir -Recurse -Force -ErrorAction Stop
}

# Ensure cfg dir
if (-not (Test-Path $cfgDir)) { New-Item -ItemType Directory -Path $cfgDir -Force | Out-Null }

# Write config
@"
[mysqld]
basedir="$base"
datadir="$dataDir"
port=3306
"@ | Set-Content -Path $cfg -Encoding ASCII
Log "Wrote $cfg"

# Initialize data dir with empty root password
Log "Initializing data dir (this takes 10-30 seconds)"
$initOut = "$env:TEMP\mysqld-init-out.log"
$initErr = "$env:TEMP\mysqld-init-err.log"
$p = Start-Process -FilePath "$base\bin\mysqld.exe" `
    -ArgumentList "--defaults-file=`"$cfg`"","--initialize-insecure" `
    -NoNewWindow -Wait -PassThru `
    -RedirectStandardOutput $initOut -RedirectStandardError $initErr
Log "init exit code: $($p.ExitCode)"
if (Test-Path $initErr) { Get-Content $initErr | Select-Object -Last 20 | ForEach-Object { Log "  [init] $_" } }
if ($p.ExitCode -ne 0) { Log "Init failed - check $initErr"; exit 1 }

# Sanity check: mysql/ system schema should exist
if (-not (Test-Path "$dataDir\mysql")) { Log "ERROR: data dir init did not create mysql/ schema"; exit 1 }
Log "Data dir initialized successfully"

# Install service
Log "Installing MySQL84 service"
$p = Start-Process -FilePath "$base\bin\mysqld.exe" `
    -ArgumentList "--install","MySQL84","--defaults-file=`"$cfg`"" `
    -NoNewWindow -Wait -PassThru
Log "install exit code: $($p.ExitCode)"

# Start service
Log "Starting MySQL84 service"
try {
    Start-Service -Name MySQL84 -ErrorAction Stop
} catch {
    Log "Start-Service failed: $_"
    # Show recent errors from mysql err log
    $errLog = Get-ChildItem $dataDir -Filter "*.err" -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($errLog) { Get-Content $errLog.FullName | Select-Object -Last 15 | ForEach-Object { Log "  [mysqld] $_" } }
    exit 1
}
Start-Sleep -Seconds 2
$st = (Get-Service MySQL84).Status
Log "Service status: $st"
if ($st -ne 'Running') { exit 1 }

Log "SUCCESS - MySQL is running"
