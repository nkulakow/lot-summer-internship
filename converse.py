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



def main():

    REQUIRED_KEYS = ["AIRCRAFT_REGISTRATION", "FLIGHT_NUMBER", "FLIGHT_DATE", "DEPARTURE_AIRPORT", "ARRIVAL_AIRPORT", "SCHEDULE_DEPARTURE_TIME", "REFUELED_AT"]


    input_folder_path = "json_folder"
    output_folder_path = "xml_folder"


    files_names = os.listdir(input_folder_path)
    # TODO check if folder exists
    files_paths = Queue()
    conversed_files = []
    bad_files = []

    def check_files():
        while True:
            time.sleep(2) 
            files_names_check_files = os.listdir(input_folder_path)
            for file_name_check_files in files_names_check_files:
                file_path_check_files = os.path.join(input_folder_path, file_name_check_files)
                file_size_check_files = os.path.getsize(file_path_check_files)
                if (file_path_check_files, file_size_check_files) not in conversed_files and (file_path_check_files, file_size_check_files) not in bad_files:
                    print (f"File {file_path_check_files, file_size_check_files} is new or was changed, adding to queue.")
                    files_paths.put(file_path_check_files)

    check_files_thread = Thread(target=check_files, daemon=True)
    check_files_thread.start()

    for file_name in files_names:
        files_paths.put(os.path.join(input_folder_path, file_name))

    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)

    while True:
        print(files_paths.qsize())
        file_path = files_paths.get()

        file_size = os.path.getsize(file_path)

        if file_path.split(".")[-1] != "json":
            print(f"File {file_path} is not a json file")
            bad_files.append((file_path, file_size ))
            continue

        with open(file_path, "r") as json_file:
            try:
                data = json.load(json_file)
            except json.JSONDecodeError as e:
                print(f"File {file_path} is not a valid json file, skipping. Raised '{e}'")
                bad_files.append((file_path, file_size ))
                continue

        df = pd.DataFrame(data.values(), index=data.keys())
        if df.empty:
            print(f"File {file_path} is empty, or data could not be imported properly, skipping.")
            bad_files.append((file_path, file_size ))
            continue

        for key in REQUIRED_KEYS:
            if key not in df.columns:
                print(f"File {file_path} does not contain required key: {key}")
                df[key] = None
        
        time.sleep(1) # Checking for file writing, set time as parameter

        if os.path.getsize(file_path) != file_size:
            print(f"File {file_path} is being written, will try again later")
            files_paths.put(file_path)
            continue

        xml_data = df.to_xml()
        output_file_path = os.path.join(output_folder_path, file_path.split("/")[-1].replace("json", "xml"))
        with open(output_file_path, "w") as xml_file:
            xml_file.write(xml_data)
        conversed_files.append((file_path, file_size))

if __name__ == "__main__":
    main()