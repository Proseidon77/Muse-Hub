
[Console]::OutputEncoding = [Text.Encoding]::UTF8
Copy-Item C:\Tools\MuseHub\start-hub.cmd C:\Tools\MuseHub\start-hub.cmd.bak -Force
$cmd = Get-Content C:\Tools\MuseHub\start-hub.cmd -Raw
$cmd = $cmd -replace 'timeout /t 5 /nobreak >nul', 'ping -n 6 127.0.0.1 >nul'
$cmd = $cmd -replace 'timeout /t 10 /nobreak >nul', 'ping -n 11 127.0.0.1 >nul'
[IO.File]::WriteAllText('C:\Tools\MuseHub\start-hub.cmd', $cmd, [Text.Encoding]::ASCII)
"--- start-hub.cmd now ---"
Get-Content C:\Tools\MuseHub\start-hub.cmd
"--- main task: principal S4U + no time limit + restart-on-failure ---"
$principal = New-ScheduledTaskPrincipal -UserId "ddPC\proseidon" -LogonType S4U -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit (New-TimeSpan -Seconds 0) -StartWhenAvailable
Set-ScheduledTask -TaskName MuseHub -Principal $principal -Settings $settings | Out-Null
$t = Get-ScheduledTask -TaskName MuseHub
"principal: " + $t.Principal.LogonType + " / " + $t.Principal.UserId
"settings: limit=" + $t.Settings.ExecutionTimeLimit + " restartCount=" + $t.Settings.RestartCount
"--- watchdog task ---"
$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\Tools\MuseHub\watchdog.ps1"
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5) -RepetitionDuration (New-TimeSpan -Days 3650)
$wprincipal = New-ScheduledTaskPrincipal -UserId "ddPC\proseidon" -LogonType S4U -RunLevel Limited
$wsettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 5)
Register-ScheduledTask -TaskName MuseHubWatchdog -Action $action -Trigger $trigger -Principal $wprincipal -Settings $wsettings -Force | Out-Null
Start-ScheduledTask -TaskName MuseHubWatchdog
Start-Sleep -Seconds 8
$wi = Get-ScheduledTaskInfo -TaskName MuseHubWatchdog
"watchdog test run: LastRunTime=" + $wi.LastRunTime + " result=" + $wi.LastTaskResult
"watchdog log exists (should be False while hub healthy): " + (Test-Path C:\Tools\MuseHub\watchdog.log)
