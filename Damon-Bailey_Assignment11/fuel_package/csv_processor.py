# File Name : csv_processor.py
# Student Name: Drew Wolfe
# email:  wolfeaw@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   4/17/2025
# Course #/Section:   4010-002
# Semester/Year:   Spring 2025
# Brief Description of the assignment:  Scrape a CSV file, reformat some parts of it,
# and use an API to fill in missing parts

# Brief Description of what this module does: Handles reading the input CSV file and writing the final
# cleaned and anomaly CSV files into the correct "Data" folder

# Citations: https://gemini.google.com/app
# Anything else that's relevant: YOU MUST OPEN THE "Data" FOLDER IN FILE EXPLORER TO SEE THE NEW CSVs


import csv
import os
import logging
from typing import Union, List, Dict, Tuple, Optional # Import Union, List, Dict, Tuple, Optional

class CSVProcessor:
    """
    Handles reading and writing CSV files for the fuel data.
    Ensures operations happen within the specified 'Data' directory.
    """
    def __init__(self, data_folder: str = 'Data'):
        """
        Initializes the CSVProcessor.

        Args:
            data_folder: The name of the folder containing data files.
                         Defaults to 'Data'.
        """
        self.data_folder_path = os.path.abspath(data_folder)
        if not os.path.exists(self.data_folder_path):
            logging.warning(f"Data folder '{self.data_folder_path}' not found. Creating it.")
            try:
                os.makedirs(self.data_folder_path)
            except OSError as e:
                logging.error(f"Could not create data folder '{self.data_folder_path}': {e}")
                raise

    # Updated type hint using Union
    def read_csv(self, filename: str) -> Union[Tuple[List[str], List[Dict[str, str]]], Tuple[None, None]]:
        """
        Reads a CSV file from the data folder into a list of dictionaries.

        Args:
            filename: The name of the CSV file within the data folder.

        Returns:
            A tuple containing (header_list, data_list_of_dicts) or (None, None) if an error occurs.
            Handles potential quote characters within data.
        """
        file_path = os.path.join(self.data_folder_path, filename)
        data: List[Dict[str, str]] = []
        header: List[str] = []

        try:
            with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                header = reader.fieldnames if reader.fieldnames else []
                if not header:
                    logging.error(f"CSV file '{filename}' appears to be empty or header is missing.")
                    return None, None

                # Explicitly cast row values to str if needed, though DictReader usually returns strings
                for row in reader:
                     data.append(dict(row)) # Ensure it's a standard dict

            logging.info(f"Successfully read {len(data)} rows from '{filename}'.")
            return header, data

        except FileNotFoundError:
            logging.error(f"Error: Input CSV file not found at '{file_path}'.")
            return None, None
        except Exception as e:
            logging.error(f"Error reading CSV file '{file_path}': {e}")
            return None, None

    # Keep header type hint general or refine if needed
    def write_csv(self, filename: str, data: List[Dict[str, any]], header: List[str]) -> bool:
        """
        Writes a list of dictionaries to a CSV file in the data folder.

        Args:
            filename: The name of the CSV file to write within the data folder.
            data: A list of dictionaries representing the rows.
            header: A list of strings for the header row.

        Returns:
            True if writing was successful, False otherwise.
        """
        if not data or not header:
            logging.warning(f"No data or header provided to write to '{filename}'. File not written.")
            # Ensure header keys match data keys if possible, or handle mismatches
            # Example check (optional):
            # if data and not all(h in data[0] for h in header):
            #     logging.warning(f"Header mismatch with data keys in '{filename}'. Check header list.")
            return False

        file_path = os.path.join(self.data_folder_path, filename)

        try:
            with open(file_path, mode='w', newline='', encoding='utf-8') as csvfile:
                # Ensure fieldnames match the provided header list
                writer = csv.DictWriter(csvfile, fieldnames=header, extrasaction='ignore') # 'ignore' prevents errors if data dict has extra keys

                writer.writeheader()
                writer.writerows(data)

            logging.info(f"Successfully wrote {len(data)} rows to '{filename}'.")
            return True

        except Exception as e:
            logging.error(f"Error writing CSV file '{file_path}': {e}")
            return False