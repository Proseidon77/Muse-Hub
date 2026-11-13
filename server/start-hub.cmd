@echo off
cd /d C:\Tools\MuseHub
:waitip
ipconfig | findstr /C:"100.116.194.77" >nul 2>&1 || (ping -n 6 127.0.0.1 >nul & goto waitip)
:run
MuseHub.exe >> hub.out.log 2>&1
ping -n 11 127.0.0.1 >nul
goto run
