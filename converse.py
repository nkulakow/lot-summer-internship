"""
Author: Nel Kułakowska
Date: 2024-04-22
"""

import time
import pandas as pd
import os
import json
from queue import Queue
from threading import Thread
import argparse
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import FileSystemEvent


REQUIRED_KEYS = ["AIRCRAFT_REGISTRATION", "FLIGHT_NUMBER", "FLIGHT_DATE", "DEPARTURE_AIRPORT", "ARRIVAL_AIRPORT", "SCHEDULE_DEPARTURE_TIME", "REFUELED_AT"]
FILES_PATHS = Queue()


class FileChangeHandler(FileSystemEventHandler):
    def on_modified(self, event : FileSystemEvent) -> None:
        file_path = event.src_path
        if file_path not in FILES_PATHS.queue and not os.path.isdir(file_path):
            print(f"\033[92m[INFO]\033[0m File {file_path} was modified at {time.ctime(os.path.getmtime(file_path))}, adding to queue.")
            logging.info(f"File {file_path} was modified at {time.ctime(os.path.getmtime(file_path))}, added to queue.")
            FILES_PATHS.put(file_path)

    def on_created(self, event : FileSystemEvent) -> None:
        file_path = event.src_path
        if file_path not in FILES_PATHS.queue and not os.path.isdir(file_path):
            print(f"\033[92m[INFO]\033[0m File {file_path} was created at {time.ctime(os.path.getmtime(file_path))}, adding to queue.")
            logging.info(f"File {file_path} was created at {time.ctime(os.path.getmtime(file_path))}, added to queue.")
            FILES_PATHS.put(file_path)


def check_files(input_folder_path: str) -> None:
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, input_folder_path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


def argument_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Converse json files to xml")
    parser.add_argument("input", type=str, help="Path to folder with json files")
    parser.add_argument("-output", "--output_folder_path", type=str, default="xml_folder", help="Path to folder where xml files will be saved, deault xml_folder")
    parser.add_argument("-tww", "--time_wait_write", type=float, default=0.1, help="Time delay for checking if file is still being written in seconds, default 0s")
    parser.add_argument("-log_a", "--log_a", type=str, default="all_logs.log", help="Path to all log file, default all_logs.log")
    parser.add_argument("-log_w", "--log_w", type=str, default="warning_logs.log", help="Path to warning log file, default warning_logs.log")
    parser.add_argument('-np', '--no_print', action='store_false', help="Disable printing")
    args = parser.parse_args()
    return args


def main():

    args = argument_parser()

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("[%(levelname)s] %(asctime)s %(message)s", datefmt="%m/%d/%Y %H:%M:%S")

    handler1 = logging.FileHandler(args.log_a)
    handler1.setFormatter(formatter)
    handler1.setLevel(logging.INFO)
    logger.addHandler(handler1)

    handler2 = logging.FileHandler(args.log_w)
    handler2.setFormatter(formatter)
    handler2.setLevel(logging.WARNING)
    logger.addHandler(handler2)

    input_folder_path = args.input
    output_folder_path = args.output_folder_path
    print_enabled = args.no_print

    print("\033[92mProgram was started by user\033[0m ") if print_enabled else None
    logging.info("Program was started by user")

    try:

        try:
            files_names = os.listdir(input_folder_path)
        except FileNotFoundError:
            print(f"\033[91mFolder {input_folder_path} does not exist\033[0m") if print_enabled else None
            logging.error(f"Folder {input_folder_path} does not exist")
            return

        check_files_thread = Thread(target=check_files, args=(input_folder_path, ), daemon=True)

        for file_name in files_names:
            FILES_PATHS.put(os.path.join(input_folder_path, file_name))

        if not os.path.exists(output_folder_path):
            os.makedirs(output_folder_path)

        thread_not_started = True

        while True:

            if thread_not_started and 0 == FILES_PATHS.qsize():
                check_files_thread.start()
                thread_not_started = False

            file_path = FILES_PATHS.get()

            last_modified = os.path.getmtime(file_path)

            if file_path.split(".")[-1] != "json":
                print(f"\033[91m[ERROR]\033[0m File {file_path} is not a json file") if print_enabled else None
                logging.error(f"File {file_path} is not a json file")
                continue

            with open(file_path, "r") as json_file:
                try:
                    data = json.load(json_file)
                except json.JSONDecodeError as e:
                    print(f"\033[91m[ERROR]\033[0m File {file_path} is not a valid json file, skipping. Raised '{e}'") if print_enabled else None
                    logging.error(f"File {file_path} is not a valid json file, skipping. Raised '{e}'")
                    continue

            df = pd.DataFrame(data.values(), index=data.keys())
            if df.empty:
                print(f"\033[91m[ERROR]\033[0m File {file_path} is empty, or data could not be imported properly, skipping.") if print_enabled else None
                logging.error(f"File {file_path} is empty, or data could not be imported properly, skipping.")
                continue

            for key in REQUIRED_KEYS:
                if key not in df.columns:
                    print(f"\033[93m[WARN]\033[0m File {file_path} does not contain required key: {key}") if print_enabled else None
                    logging.warning(f"File {file_path} does not contain required key: {key}")
                    df[key] = None

            time.sleep(args.time_wait_write)

            if os.path.getmtime(file_path) != last_modified:
                print(f"\033[93m[WARN]\033[0m File {file_path} is being written, will try again later") if print_enabled else None
                logging.warning(f"File {file_path} is being written, will try again later")
                FILES_PATHS.put(file_path)
                continue

            xml_data = df.to_xml()
            output_file_path = os.path.join(output_folder_path, file_path.split("/")[-1].replace("json", "xml"))
            with open(output_file_path, "w") as xml_file:
                xml_file.write(xml_data)
            print(f"\033[92m[INFO]\033[0m File {file_path} was converted to {output_file_path}") if print_enabled else None
            logging.info(f"File {file_path} was converted to {output_file_path}")

    except KeyboardInterrupt:
        print("\033[92mProgram was stopped by user\033[0m ") if print_enabled else None
        logging.info("Program was stopped by user")
        return


if __name__ == "__main__":
    main()
