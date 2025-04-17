# main.py
# Name: Connor Thomas
# Email: thoma5cg@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date: 4/17/25
# Course #/Section: IS4010/002
# Semester/Year: 2nd/4th
# Brief description of the assignment: In this assignment, we are cleaning data.
# Brief description of what this module does: This module controls the data cleaning process for a fuel purchase dataset.
# Citations: ChatGPT
# Anything else that's relevant:

import logging
import os
from fuel_package.csv_processor import CSVProcessor
from fuel_package.utilities import DataCleaner
from fuel_package.zip_lookup import ZipLookup

# Configure basic logging (DEBUG shows city extraction details)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def main():
    """
    Main function to orchestrate the fuel data cleaning process,
    handling specific 'Full Address' column formats.
    """
    logging.info("Starting data cleaning process...")

    # --- Configuration ---
    data_folder = 'Data'
    input_csv_filename = 'fuelPurchaseData.csv'
    cleaned_output_filename = 'cleanedData.CSV'
    anomalies_output_filename = 'dataAnomalies.CSV'
    max_zip_lookups = 5

    # --- Instantiate Processors ---
    csv_processor = CSVProcessor(data_folder=data_folder)
    zip_lookup = ZipLookup()

    # --- Read Input Data ---
    header, data = csv_processor.read_csv(input_csv_filename)
    if data is None or header is None:
        logging.error("Failed to read input data. Exiting.")
        return
    full_address_col = next((h for h in header if h.strip().lower() == 'full address'), None)
    if not full_address_col:
        logging.warning("Input data does not seem to have a 'Full Address' column. Zip lookup/update may not work.")

    # --- Clean Data (Duplicates, Price, Anomalies) ---
    cleaner = DataCleaner(data)
    cleaner.header = header # Pass the header read from CSV
    cleaned_data, anomalies = cleaner.clean_data()

    # --- Identify Rows Needing Zip Lookup (Based on improved logic) ---
    rows_missing_zip = cleaner.get_rows_missing_zip()

    # --- Perform Zip Lookups (Limited) ---
    zip_lookups_attempted = 0
    if not full_address_col:
         logging.warning("Skipping zip lookup because 'Full Address' column was not found.")
    elif not rows_missing_zip:
         logging.info("No rows found needing zip code lookup based on 'Full Address'.")
    else:
        logging.info(f"Attempting to look up missing zip codes for up to {max_zip_lookups} rows...")
        for index, row in rows_missing_zip:
            if zip_lookups_attempted >= max_zip_lookups:
                logging.info(f"Reached maximum ({max_zip_lookups}) zip code lookups.")
                break
            current_address = row.get(full_address_col)

            # Attempt to extract city using the improved method in DataCleaner
            city = cleaner.extract_city_from_address(current_address)
            if city:
                 logging.info(f"Looking up zip for extracted city: '{city}' (Row index in cleaned data: {index})")
                 found_zip = zip_lookup.get_zip_for_city(city)
                 zip_lookups_attempted += 1
                 if found_zip:

                     # Append zip code to the 'Full Address' since missing ones should be at the end
                     address_to_update = cleaned_data[index].get(full_address_col, "")
                     if not isinstance(address_to_update, str):
                         address_to_update = str(address_to_update)

                     # Append with a space, ensure not to add if somehow already present (unlikely given get_rows_missing_zip)
                     if not cleaner._address_has_zip(address_to_update): # Double check it's still missing
                         updated_address = f"{address_to_update.strip()} {found_zip}".strip()
                         cleaned_data[index][full_address_col] = updated_address
                         logging.info(f"Appended zip '{found_zip}' to address for city '{city}'. New address: '{updated_address}'")
                     else:
                          logging.warning(f"Zip '{found_zip}' found by API, but address '{address_to_update}' now appears to have a zip already. No update made.")
                 else:
                     logging.warning(f"Could not find zip via API for extracted city: '{city}' from address '{current_address}'")
            else:
                 logging.warning(f"Could not extract city from address at index {index} to perform zip lookup. Address: '{current_address}'")

                 # Increment attempt counter even if city extraction fails
                 zip_lookups_attempted += 1

    # --- Write Output Files ---
    logging.info("Writing output files...")
    success_cleaned = csv_processor.write_csv(cleaned_output_filename, cleaned_data, header)
    success_anomalies = csv_processor.write_csv(anomalies_output_filename, anomalies, header)
    if success_cleaned:
        logging.info(f"Cleaned data saved to '{os.path.join(data_folder, cleaned_output_filename)}'")
    else:
        logging.error("Failed to write cleaned data file.")
    if success_anomalies:
        logging.info(f"Anomalies saved to '{os.path.join(data_folder, anomalies_output_filename)}'")
    else:
        logging.warning("Failed to write anomalies file.")
    logging.info("Data cleaning process finished.")

    # --- Extra Credit ---
    logging.info("\n--- Extra Credit Notes ---")
    logging.info("Consider checking for:")
    logging.info("- Addresses that don't match the two expected formats.")
    logging.info("- Incorrect city extraction despite matching formats (e.g., 'St. Louis').")
    logging.info("- Outlier values in numerical columns.")
    logging.info("- Date/time format inconsistencies.")
if __name__ == "__main__":
    main()