import os
import json
import time
import logging
import argparse
from datetime import datetime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pydicom
import requests
import re


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
    def __init__(self, configs):
        self.configs = configs

    def on_created(self, event):
        self.process(event)

    def on_modified(self, event):
        self.process(event)

    def process(self, event):
        # Only process files that are newly created or modified
        if event.is_directory:
            return
        if event.src_path.lower().endswith(".dcm"):
            self.handle_dicom_file(event.src_path)

    def handle_dicom_file(self, file_path):
        try:
            ds = pydicom.dcmread(file_path)
            study_description = getattr(ds, "StudyDescription", "")
            norm_study_description = normalize_string(study_description)
            for config in self.configs:
                norm_config_study_description = normalize_string(
                    config["study_description"]
                )
                if norm_study_description == norm_config_study_description:
                    self.send_to_api(file_path, config["api_endpoint"])
        except Exception as e:
            logging.error(f"Failed to process {file_path}: {e}")

    def send_to_api(self, file_path, api_endpoint):
        try:
            with open(file_path, "rb") as f:
                response = requests.post(api_endpoint, files={"file": f})
                if response.status_code == 200:
                    logging.info(
                        f"Successfully sent {file_path} to API at {api_endpoint}."
                    )
                else:
                    logging.error(
                        f"Failed to send {file_path} to API. Status code: {response.status_code}"
                    )
        except Exception as e:
            logging.error(f"Failed to send {file_path} to API: {e}")


class DicomFileDeleter(FileSystemEventHandler):
    def __init__(self, watch_dir, max_age_days=14):
        self.watch_dir = watch_dir
        self.max_age_days = max_age_days

    def on_created(self, event):
        pass

    def on_modified(self, event):
        pass

    def on_deleted(self, event):
        if not event.is_directory and event.src_path.lower().endswith(".dcm"):
            self.delete_file(event.src_path)

    def on_moved(self, event):
        pass

    def delete_file(self, file_path):
        try:
            os.remove(file_path)
            logging.info(f"Deleted DICOM file: {file_path}")

            # Check if parent directory becomes empty
            parent_dir = os.path.dirname(file_path)
            if not os.listdir(parent_dir):
                os.rmdir(parent_dir)
                logging.info(f"Removed empty directory: {parent_dir}")

        except Exception as e:
            logging.error(f"Failed to delete DICOM file: {e}")

    def check_and_delete_old_files(self):
        try:
            now = datetime.now()
            for root, _, files in os.walk(self.watch_dir):
                for file_name in files:
                    if file_name.lower().endswith(".dcm"):
                        file_path = os.path.join(root, file_name)
                        modified_time = datetime.fromtimestamp(
                            os.path.getmtime(file_path)
                        )
                        if now - modified_time > timedelta(days=self.max_age_days):
                            self.delete_file(file_path)
        except Exception as e:
            logging.error(f"Failed to delete old DICOM files: {e}")


def main():
    # Default paths
    default_settings_file = os.path.join(
        os.path.dirname(__file__), "custom", "settings.json"
    )
    default_log_dir = os.path.join(os.path.dirname(__file__), "logs")
    WATCH_DIR = os.path.join(os.path.dirname(__file__), "data")

    parser = argparse.ArgumentParser(
        description="Monitor directories for DICOM files and send them to corresponding APIs."
    )
    parser.add_argument(
        "--settings",
        type=str,
        default=default_settings_file,
        help=f"Path to the settings JSON file (default: {default_settings_file})",
    )
    parser.add_argument(
        "--logdir",
        type=str,
        default=default_log_dir,
        help=f"Directory to store log files (default: {default_log_dir})",
    )
    parser.add_argument(
        "--maxage",
        type=int,
        default=30,
        help="Maximum age of DICOM files in days to delete (default: 30)",
    )
    args = parser.parse_args()

    SETTINGS_FILE = args.settings
    LOG_DIR = args.logdir
    LOG_FILE = os.path.join(LOG_DIR, "dicom_monitor.log")

    # Ensure the log directory exists
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
    )

    settings = load_settings(SETTINGS_FILE)

    if not settings:
        logging.error("No valid configurations found in settings.json.")
        exit(1)

    watch_dirs = {config["watch_dir"] for config in settings}
    event_handler = DicomFileHandler(settings)

    # Observer for monitoring DICOM files
    observer = Observer()
    for watch_dir in watch_dirs:
        logging.info(f"Watching directory: {watch_dir}")
        observer.schedule(event_handler, path=watch_dir, recursive=True)

    observer.start()

    # Observer for deleting old DICOM files
    deleter = DicomFileDeleter(WATCH_DIR, args.maxage)
    observer_delete = Observer()
    observer_delete.schedule(deleter, path=WATCH_DIR, recursive=True)
    observer_delete.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer_delete.stop()

    observer.join()
    observer_delete.join()


if __name__ == "__main__":
    main()
