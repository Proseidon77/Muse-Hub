
$listening = Get-NetTCPConnection -LocalPort 8767 -State Listen -ErrorAction SilentlyContinue
$proc = Get-Process MuseHub -ErrorAction SilentlyContinue
if (-not $listening -or -not $proc) {
  Add-Content C:\Tools\MuseHub\watchdog.log ("{0} hub down (proc={1} listen={2}) -> starting task MuseHub" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), [bool]$proc, [bool]$listening)
  Start-ScheduledTask -TaskName MuseHub
}
