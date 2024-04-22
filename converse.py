"""
Opis business case'a:
Zgłasza się do Ciebie kierownik działu odpowiedzialnego za zarządzanie procesem tankowania
samolotów. Podczas wdrażania nowego oprogramowania wspierającego ich pracę, okazało się, że część
danych docierających od dostawców nie jest kompatybilna z nowym oprogramowaniem. Prosi Cię, o
przygotowanie rozwiązania, które umożliwi dostosowanie danych do wymaganego formatu.
Opis zadania:
1. Przygotuj skrypt w języku python (wersja 3.6 lub wyższa), który wykona konwersje plików JSON
w folderze wejściowym, do formatu XML, a następnie po dokonanej konwersji przeniesie go do
innego folderu.
Zwróć uwagę na:
a. Przed wykonaniem konwersji, upewnij się, że plik wejściowy został już w całości
zapisany.
b. Pamiętaj o tym, że może pojawić się potrzeba weryfikacji przebiegu działania konwersji
dla konkretnego pliku.
c. Obsługa błędów jest ważna, jeden błąd nie może zablokować całości przepływu.
d. Dane powinny być procesowane możliwie na bieżąco.
e. Unikaj umieszczania dynamicznych parametrów w kodzie (np. ścieżki do folderów).
2. Zaproponuj:
a. Co powinno być objęte przez system monitoringu, żebyśmy wiedzieli, że w naszym
przepływie coś nie działa poprawnie.
b. W jaki sposób powinno być uruchamiane rozwiązanie na serwerze, gdzie zostanie
umieszczone.
Przykład pliku wejściowego:
{
 "FLIGHT": {
 "AIRCRAFT_REGISTRATION": "SPLSA",
 "FLIGHT_NUMBER": 458,
 "FLIGHT_DATE": "2024-03-20",
 "DEPARTURE_AIRPORT": "WAW",
 "ARRIVAL_AIRPORT": "JFK",
 "SCHEDULE_DEPARTURE_TIME": "2024-03-20 13:00:00",
 "REFUELED_AT": "2024-03-20 12:30:00"
 }
}
"""

import time
import pandas as pd
import os
import json
from queue import Queue
from threading import Thread
import argparse
import logging
import subprocess


REQUIRED_KEYS = ["AIRCRAFT_REGISTRATION", "FLIGHT_NUMBER", "FLIGHT_DATE", "DEPARTURE_AIRPORT", "ARRIVAL_AIRPORT", "SCHEDULE_DEPARTURE_TIME", "REFUELED_AT"]
FILES_PATHS = Queue()
CONVERSED_FILES = []
BAD_FILES = []


def argument_parser():
    parser = argparse.ArgumentParser(description="Converse json files to xml")
    parser.add_argument("input", type=str, help="Path to folder with json files")
    parser.add_argument("-output", "--output_folder_path", type=str, default="xml_folder", help="Path to folder where xml files will be saved, deault xml_folder")
    parser.add_argument("-tdc", "--time_delay_check", type=float, default=1, help="Time delay for checking for new/modified files in seconds, default 2s")
    parser.add_argument("-tww", "--time_wait_write", type=float, default=0.1, help="Time delay for checking if file is still being written in seconds, default 0s")
    parser.add_argument("-log_a", "--log_a", type=str, default="all_logs.log", help="Path to all log file, default all_logs.log")
    parser.add_argument("-log_w", "--log_w", type=str, default="warning_logs.log", help="Path to warning log file, default warning_logs.log")
    parser.add_argument("-p", "--print", type=int, default=1, help="Enable or disable printing, default 1 (enabled), to disable set to 0")
    args = parser.parse_args()
    return args


def check_files(input_folder_path: str, time_delay_check: float) -> None:
    while True:
        time.sleep(time_delay_check)
        files_names_check_files = os.listdir(input_folder_path)
        for file_name_check_files in files_names_check_files:
            file_path_check_files = os.path.join(input_folder_path, file_name_check_files)
            file_last_modified_check_files = os.path.getmtime(file_path_check_files)
            if (file_path_check_files, file_last_modified_check_files) not in CONVERSED_FILES and (file_path_check_files, file_last_modified_check_files) not in BAD_FILES:
                print(f"\033[92m[INFO]\033[0m File {file_path_check_files} is new or was changed at {time.ctime(file_last_modified_check_files)}, adding to queue.")
                FILES_PATHS.put(file_path_check_files)

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
    print_enabled = args.print

    print("\033[92mProgram was started by user\033[0m ") if print_enabled else None
    logging.info("Program was started by user")

    try:

        try:
            files_names = os.listdir(input_folder_path)
        except FileNotFoundError:
            print(f"\033[91mFolder {input_folder_path} does not exist\033[0m") if print_enabled else None
            logging.error(f"Folder {input_folder_path} does not exist")
            return

        check_files_thread = Thread(target=check_files, args=(input_folder_path, args.time_delay_check), daemon=True)

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
                BAD_FILES.append((file_path, last_modified))
                continue

            with open(file_path, "r") as json_file:
                try:
                    data = json.load(json_file)
                except json.JSONDecodeError as e:
                    print(f"\033[91m[ERROR]\033[0m File {file_path} is not a valid json file, skipping. Raised '{e}'") if print_enabled else None
                    logging.error(f"File {file_path} is not a valid json file, skipping. Raised '{e}'")
                    BAD_FILES.append((file_path, last_modified))
                    continue

            df = pd.DataFrame(data.values(), index=data.keys())
            if df.empty:
                print(f"\033[91m[ERROR]\033[0m File {file_path} is empty, or data could not be imported properly, skipping.") if print_enabled else None
                logging.error(f"File {file_path} is empty, or data could not be imported properly, skipping.")
                BAD_FILES.append((file_path, last_modified))
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
            CONVERSED_FILES.append((file_path, last_modified))
    
    except KeyboardInterrupt:
        print("\033[92mProgram was stopped by user\033[0m ") if print_enabled else None
        logging.info("Program was stopped by user")
        return


if __name__ == "__main__":
    main()
