@echo off
REM Change to the directory of this batch file (the Python script directory)
cd /d "%~dp0"

REM Activate the pipenv environment
call pipenv shell

REM Run the Python script in the background
start /b python dicom_monitor.py --settings path\to\settings.json --logdir path\to\logs

REM Exit the pipenv shell
exit