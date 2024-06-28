@echo off
REM Activate the pipenv environment to get the path to python.exe
REM You need to replace `your_project` with your actual project name
pipenv run where python > python_path.txt

REM Read the path from the text file
set /p python_path=<python_path.txt

REM Remove the filename to get the directory
for %%f in ("%python_path%") do set python_dir=%%~dpf

REM Copy and rename python.exe
copy "%python_path%" "%python_dir%python-dicom-monitor2.exe"

REM Clean up
del python_path.txt

echo Python executable copied and renamed to python-dicom-monitor.exe