# File Name : utilities.py
# Student Name: Drew Wolfe
# email:  wolfeaw@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   4/17/2025
# Course #/Section:   4010-002
# Semester/Year:   Spring 2025
# Brief Description of the assignment:  Scrape a CSV file, reformat some parts of it, 
# and use an API to fill in missing parts

# Brief Description of what this module does: Cleans the data by formatting prices, removing duplicates,
# identifying rows needing zip codes, and attempting to extract city names from addresses

# Citations: https://gemini.google.com/app
# Anything else that's relevant: YOU MUST OPEN THE "Data" FOLDER IN FILE EXPLORER TO SEE THE NEW CSVs


import logging
import re # Import regular expression module
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import List, Dict, Tuple, Any, Optional

class DataCleaner:
    """
    Provides methods for cleaning fuel purchase data, including handling
    a combined 'Full Address' column and using 'Fuel Type' for anomalies.
    """

    def __init__(self, data: List[Dict[str, Any]]):
        self.data = data
        self.header = [str(k) for k in data[0].keys()] if data else []
        self.cleaned_data: List[Dict[str, Any]] = []
        self.anomalies: List[Dict[str, Any]] = []
        self._processed_hashes = set()
        # Regex to find a 5 or 9 digit zip code at the START of a string
        self.zip_regex_start = re.compile(r'^\s*(\d{5}(?:-\d{4})?)\b')
        # Regex to find a 5 or 9 digit zip code at the END of a string
        self.zip_regex_end = re.compile(r'\b(\d{5}(?:-\d{4})?)\s*$')


    def _calculate_row_hash(self, row: Dict[str, Any]) -> int:
        # Calculates a hash for a row to detect duplicates.
        try:
            # Use header for consistent key order
            return hash(tuple(str(row.get(col, '')) for col in self.header))
        except TypeError as e:
             logging.error(f"Hashing error for row {row}: {e}. Using fallback hash.")
             # Fallback: hash based on a subset of known string columns or just repr
             return hash(repr(row))


    def clean_data(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Performs cleaning: duplicates, anomalies (Pepsi in 'Fuel Type'), formats 'Gross Price'.
        Does NOT handle zip codes here, that's done after cleaning.
        """
        self.cleaned_data = []
        self.anomalies = []
        self._processed_hashes = set()

        if not self.data:
            logging.warning("Input data is empty. No cleaning performed.")
            return [], []

        if not self.header:
             logging.error("Cannot determine headers from data.")
             return [], []

        # --- Column Name Identification ---
        price_col = next((h for h in self.header if h.strip().lower() == 'gross price'), None)
        # ***** CHANGE HERE: Look for 'Fuel Type' instead of 'Product' *****
        fuel_type_col = next((h for h in self.header if h.strip().lower() == 'fuel type'), None)
        # Keep looking for 'Full Address'
        full_address_col = next((h for h in self.header if h.strip().lower() == 'full address'), None)

        if not price_col: logging.warning("Could not find 'Gross Price' column.")
        # ***** CHANGE HERE: Update warning message *****
        if not fuel_type_col: logging.warning("Could not find 'Fuel Type' column. Cannot check for Pepsi anomalies.")
        if not full_address_col: logging.warning("Could not find 'Full Address' column.")


        for row_index, row in enumerate(self.data):
            row_hash = self._calculate_row_hash(row)
            if row_hash in self._processed_hashes:
                # Don't log every single duplicate if there are many - maybe sample or summarize later?
                # logging.info(f"Duplicate row skipped (original index: {row_index}): {row}")
                continue # Skip duplicate processing
            self._processed_hashes.add(row_hash)

            cleaned_row = row.copy()

            # --- Anomaly Check (Pepsi) ---
            is_anomaly = False
            # ***** CHANGE HERE: Use fuel_type_col *****
            fuel_type_value = cleaned_row.get(fuel_type_col, '')
            # Check type before calling string methods
            if fuel_type_col and isinstance(fuel_type_value, str) and \
               'pepsi' in fuel_type_value.lower():
                # Log only the first few anomalies detected to avoid flooding logs?
                # Or just count them and log summary later.
                logging.info(f"Anomaly detected (Pepsi in 'Fuel Type') at original index {row_index}: {row}")
                self.anomalies.append(cleaned_row)
                is_anomaly = True


            # --- Format Gross Price ---
            if price_col and price_col in cleaned_row:
                price_value = cleaned_row[price_col]
                try:
                    price_str = str(price_value).replace('$', '').replace(',', '').strip()
                    if price_str:
                        price_decimal = Decimal(price_str)
                        cleaned_row[price_col] = str(price_decimal.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                    else:
                         cleaned_row[price_col] = "0.00"
                         logging.warning(f"Empty or invalid price string '{price_value}' at original index {row_index}. Setting to '0.00'.")
                except (InvalidOperation, ValueError, TypeError) as e:
                    logging.warning(f"Could not parse price '{price_value}' at original index {row_index}: {e}. Leaving as is: '{cleaned_row[price_col]}'.")

            # Add to the appropriate list ONLY IF IT'S NOT an anomaly
            if not is_anomaly:
                self.cleaned_data.append(cleaned_row)

        # Log summary after the loop
        num_duplicates_found = len(self.data) - len(self.cleaned_data) - len(self.anomalies)
        logging.info(f"Duplicate rows skipped: {num_duplicates_found}")
        logging.info(f"Cleaning complete. Found {len(self.cleaned_data)} valid rows and {len(self.anomalies)} anomalies (Pepsi).")
        return self.cleaned_data, self.anomalies

    def _address_has_zip(self, address: Optional[str]) -> bool:
        """
        Checks if the address string contains a zip code at the beginning OR end.
        """
        if not address or not isinstance(address, str):
            return False
        address_stripped = address.strip()
        has_zip_start = bool(self.zip_regex_start.search(address_stripped))
        has_zip_end = bool(self.zip_regex_end.search(address_stripped))
        return has_zip_start or has_zip_end

    def extract_city_from_address(self, address: Optional[str]) -> Optional[str]:
        """
        Extracts the city name from 'Full Address' based on the two known formats.
        Format 1: "ZIP ST, City, Street,"
        Format 2: "Street, City, ST ZIP" or "Street, City, ST" (when zip missing)
        """
        if not address or not isinstance(address, str):
            return None

        address = address.strip()
        parts = [p.strip() for p in address.split(',') if p.strip()]

        # Check for Format 1 (Starts with Zip)
        if self.zip_regex_start.search(address):
            if len(parts) >= 2:
                city = parts[1] # City is the second element
                logging.debug(f"Extracted city '{city}' from Format 1 address: '{address}'")
                return city.strip()
            else:
                logging.warning(f"Address starts with zip but has < 2 parts after comma split: '{address}'")
                return None
        # Check for Format 2 or Missing Zip (Zip should be at end if present)
        else:
            if len(parts) >= 2:
                # City is expected to be the second-to-last element
                city = parts[-2]
                logging.debug(f"Extracted city '{city}' from Format 2/Missing address: '{address}'")
                return city.strip()
            elif len(parts) == 1:
                 logging.warning(f"Address has only one part after comma split, cannot reliably determine city: '{address}'")
                 return None
            else:
                 logging.warning(f"Address does not start with zip and has < 2 parts after comma split: '{address}'")
                 return None

    def get_rows_missing_zip(self) -> List[Tuple[int, Dict[str, Any]]]:
        """
        Identifies rows in cleaned_data missing a zip code in 'Full Address',
        checking both start and end positions.
        """
        missing_zip_rows = []
        full_address_col = next((h for h in self.header if h.strip().lower() == 'full address'), None)

        if not full_address_col:
            logging.error("Cannot identify rows missing zip codes without a 'Full Address' column.")
            return []

        for index, row in enumerate(self.cleaned_data):
            address_value = row.get(full_address_col)
            if not self._address_has_zip(address_value):
                 logging.debug(f"Row index {index} identified as missing zip. Address: '{address_value}'")
                 missing_zip_rows.append((index, row))

        logging.info(f"Found {len(missing_zip_rows)} rows potentially missing zip codes in 'Full Address'.")
        return missing_zip_rows
