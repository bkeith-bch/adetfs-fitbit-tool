REM @echo off
powershell -window minimized -command ""
SET /A counter = 0

:ping
ping -n 1 www.google.com -w 1000 > null && goto a
REM No internet access or access too slow. Sleep for 10 minutes
timeout /t 600 /nobreak
SET /A counter +=1
IF %counter% LSS 5 (
goto ping)
IF %counter% EQU 5 (
echo "%date% Software did not execute because no internet connection after %counter% tries">> error.log
powershell -window normal -command ""
exit /b)

:a
@REM Your connected script here
cd /d E:\path_to_folder_that_contains_venv\.venv\Scripts\ & activate & cd /d E:\path_to_software_folder\adetfs\ & E:\path_to_folder_that_contains_venv\.venv\Scripts\python.exe -m fitbit_main
powershell -window normal -command ""

