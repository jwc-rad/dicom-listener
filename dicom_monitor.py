import os
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pydicom
from setproctitle import *
import requests
import re
import threading

setproctitle('Dicom Monitor')

def load_settings(settings_file):
    try:
        with open(settings_file, "r") as f:
            settings = json.load(f)
            return settings
    except FileNotFoundError:
        logging.error(f"Settings file '{settings_file}' not found.")
        return []
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from the settings file '{settings_file}'.")
        return []

def normalize_string(s):
    # Remove special characters and spaces, convert to lowercase
    return re.sub(r"[^a-zA-Z0-9]", "", s).lower()


class DicomFileHandler(FileSystemEventHandler):
    def __init__(self, configs, check_interval, stable_duration, api_check_interval):
        self.configs = configs
        self.check_interval = check_interval
        self.stable_duration = stable_duration
        self.api_check_interval = api_check_interval
        self.modified_files = set()
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.api_thread = threading.Thread(target=self.process_files_periodically)
        self.api_thread.daemon = True
        self.api_thread.start()

    def on_modified(self, event):
        if not event.is_directory and event.src_path.lower().endswith('.dcm'):
            with self.lock:
                if self.is_file_stable(event.src_path):
                    self.modified_files.add(event.src_path)

    def process_files_periodically(self):
        while not self.stop_event.is_set():
            with self.lock:
                files_to_process = list(self.modified_files)
                self.modified_files.clear()

            for file_path in files_to_process:
                self.handle_dicom_file(file_path)

            time.sleep(self.api_check_interval)

    def is_file_stable(self, file_path):
        previous_size = -1
        stable_time = 0
        while stable_time < self.stable_duration:
            current_size = os.path.getsize(file_path)
            if current_size != previous_size:
                stable_time = 0
                previous_size = current_size
            else:
                stable_time += self.check_interval
            time.sleep(self.check_interval)
        return True

    def handle_dicom_file(self, file_path):
        try:
            ds = pydicom.dcmread(file_path)
            study_description = getattr(ds, 'StudyDescription', '')
            norm_study_description = normalize_string(study_description)
            for config in self.configs:
                norm_config_study_description = normalize_string(config['study_description'])
                if norm_study_description == norm_config_study_description:
                    self.send_to_api(file_path, config['api_endpoint'])
        except Exception as e:
            logging.error(f"Failed to process {file_path}: {e}")

    def send_to_api(self, file_path, api_endpoint):
        try:
            files = {
                'image': (os.path.basename(file_path), open(file_path, 'rb')),
            }
            response = requests.post(api_endpoint, files=files)
            if response.status_code == 200:
                logging.info(f"Successfully sent {file_path} to API at {api_endpoint}.")
            else:
                logging.error(f"Failed to send {file_path} to API. Status code: {response.status_code}")
        except Exception as e:
            logging.error(f"Failed to send {file_path} to API: {e}")

    def stop(self):
        self.stop_event.set()
        self.api_thread.join()


class DicomFileDeleter:
    def __init__(self, watch_dirs, max_age_days=14, check_interval=86400):
        self.watch_dirs = watch_dirs
        self.max_age_days = max_age_days
        self.check_interval = check_interval
        self.stop_event = threading.Event()
        logging.info(f"Watching directories for deleting old DICOM files: {watch_dirs}")

    def delete_old_files(self):
        while not self.stop_event.is_set():
            now = datetime.now()
            try:
                for watch_dir in self.watch_dirs:
                    for root, _, files in os.walk(watch_dir):
                        for file_name in files:
                            if file_name.lower().endswith(".dcm"):
                                file_path = os.path.join(root, file_name)
                                modified_time = datetime.fromtimestamp(
                                    os.path.getmtime(file_path)
                                )
                                if now - modified_time > timedelta(
                                    days=self.max_age_days
                                ):
                                    self.delete_file(file_path)
            except Exception as e:
                logging.error(f"Failed to delete old DICOM files: {e}")

            self.stop_event.wait(self.check_interval)

    def delete_file(self, file_path):
        try:
            os.remove(file_path)
            logging.info(f"Deleted DICOM file: {file_path}")

            parent_dir = os.path.dirname(file_path)
            if not os.listdir(parent_dir):
                os.rmdir(parent_dir)
                logging.info(f"Removed empty directory: {parent_dir}")

        except Exception as e:
            logging.error(f"Failed to delete DICOM file: {e}")

    def stop(self):
        self.stop_event.set()


def main():
    default_settings_file = os.path.join(os.path.dirname(__file__), 'custom', 'settings.json')
    default_log_dir = os.path.join(os.path.dirname(__file__), 'logs')

    parser = argparse.ArgumentParser(description="Monitor directories for DICOM files and send them to corresponding APIs.")
    parser.add_argument('--settings', type=str, default=default_settings_file, help=f'Path to the settings JSON file (default: {default_settings_file})')
    parser.add_argument('--logdir', type=str, default=default_log_dir, help=f'Directory to store log files (default: {default_log_dir})')
    parser.add_argument('--maxage', type=int, default=14, help='Maximum age of DICOM files in days to delete (default: 14)')
    parser.add_argument('--checkinterval', type=int, default=86400, help='Interval in seconds to check for outdated files (default: 86400 seconds or 1 day)')
    parser.add_argument('--filecheckinterval', type=float, default=0.2, help='Interval in seconds to check if a file is stable (default: 0.2 seconds)')
    parser.add_argument('--filestableduration', type=float, default=0.6, help='Duration in seconds for which a file should be stable (default: 0.6 seconds)')
    parser.add_argument('--apicheckinterval', type=float, default=3, help='Interval in seconds to check for modified files to process (default: 3 seconds)')
    args = parser.parse_args()

    SETTINGS_FILE = args.settings
    LOG_DIR = args.logdir
    # Get the current process ID
    pid = os.getpid()
    LOG_FILE = os.path.join(LOG_DIR, f"dicom_monitor_pid_{pid}.log")

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )

    settings = load_settings(SETTINGS_FILE)

    if not settings:
        logging.error("No valid configurations found in settings.json.")
        exit(1)

    watch_dirs = {config['watch_dir'] for config in settings}
    event_handler = DicomFileHandler(settings, args.filecheckinterval, args.filestableduration, args.apicheckinterval)

    observer = Observer()
    for watch_dir in watch_dirs:
        logging.info(f"Watching directory: {watch_dir}")
        observer.schedule(event_handler, path=watch_dir, recursive=True)

    observer.start()

    deleter = DicomFileDeleter(watch_dirs, args.maxage, args.checkinterval)
    delete_thread = threading.Thread(target=deleter.delete_old_files)
    delete_thread.daemon = True
    delete_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        event_handler.stop()
        deleter.stop()
        delete_thread.join()

    observer.join()

if __name__ == "__main__":
    main()