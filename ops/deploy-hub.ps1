$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$dir = 'C:\Tools\MuseHub'
Set-Location -LiteralPath $dir
$utf8 = New-Object System.Text.UTF8Encoding($false)

# ---- stage 1: backups ----
foreach ($f in @('MuseHub.exe','client_for_family.py','README-family-muse.txt')) {
  $bak = "$f.v1.bak"
  if (-not (Test-Path -LiteralPath $bak)) { Copy-Item -LiteralPath $f -Destination $bak }
}
if (-not (Test-Path -LiteralPath 'hubconfig.v1.json')) { Copy-Item -LiteralPath 'hubconfig.json' -Destination 'hubconfig.v1.json' }
"BACKUP_OK"

# ---- stage 2: config migration v1 -> v2 (+ pre-provision A003) ----
$old = Get-Content -LiteralPath 'hubconfig.json' -Raw -Encoding UTF8 | ConvertFrom-Json
$rng = [Security.Cryptography.RandomNumberGenerator]::Create()
$bytes = New-Object byte[] 24
$rng.GetBytes($bytes)
$s3 = -join ($bytes | ForEach-Object { $_.ToString('x2') })
$agents = [ordered]@{
  'A001' = @{ name = 'sword';   secret = [string]$old.sword }
  'A002' = @{ name = 'family';  secret = [string]$old.family }
  'A003' = @{ name = 'agent-3'; secret = $s3 }
}
$json = @{ agents = $agents } | ConvertTo-Json -Depth 5
[IO.File]::WriteAllText((Join-Path $dir 'hubconfig.json'), $json, $utf8)
"CONFIG_OK"

# ---- stage 3: README with the real A002 secret substituted on-PC ----
$tpl = Get-Content -LiteralPath 'README-family-muse.txt.new' -Raw -Encoding UTF8
$tpl = $tpl.Replace('__SECRET__', [string]$old.family)
[IO.File]::WriteAllText((Join-Path $dir 'README-family-muse.txt'), $tpl, $utf8)
Remove-Item -LiteralPath 'README-family-muse.txt.new' -Force
"README_OK"

# ---- stage 4: stop old hub ----
schtasks /end /tn MuseHub 2>&1 | Out-Null
Start-Sleep -Seconds 2
Get-Process MuseHub -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Get-CimInstance Win32_Process -Filter "Name = 'cmd.exe'" | Where-Object { $_.CommandLine -match 'start-hub' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep -Seconds 2
$still = Get-Process MuseHub -ErrorAction SilentlyContinue
if ($still) { "STOP_FAILED"; exit 1 }
"STOP_OK"

# ---- stage 5: swap files ----
Move-Item -LiteralPath 'MuseHub.exe.new' -Destination 'MuseHub.exe' -Force
Move-Item -LiteralPath 'client_for_family.py.new' -Destination 'client_for_family.py' -Force
"SWAP_OK"

# ---- stage 6: start + verify ----
schtasks /run /tn MuseHub | Out-Null
Start-Sleep -Seconds 12
$p = Get-Process MuseHub -ErrorAction SilentlyContinue
if (-not $p) { "START_FAILED_NO_PROCESS"; exit 1 }
"PROCESS_OK pid=" + $p.Id
$c = Get-NetTCPConnection -LocalPort 8767 -State Listen -ErrorAction SilentlyContinue
if ($c) { "LISTEN_OK " + $c[0].LocalAddress + ":8767" } else { "LISTEN_MISSING" }
try {
  $resp = Invoke-WebRequest -Uri 'http://100.116.194.77:8767/hub/negotiate?negotiateVersion=1' -Method Post -Body '{}' -ContentType 'application/json' -TimeoutSec 10 -UseBasicParsing
  "PROBE_OK status=" + $resp.StatusCode
} catch { "PROBE_ERR " + $_.Exception.Message }
Get-ChildItem -LiteralPath $dir -Force | Sort-Object Name | ForEach-Object { "{0,12}  {1}" -f $_.Length, $_.Name }
