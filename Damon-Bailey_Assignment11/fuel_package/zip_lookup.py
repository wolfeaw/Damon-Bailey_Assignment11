# File Name : zip_lookup.py
# Student Name: Drew Wolfe
# email:  wolfeaw@mail.uc.edu
# Assignment Number: Assignment 11
# Due Date:   4/17/2025
# Course #/Section:   4010-002
# Semester/Year:   Spring 2025
# Brief Description of the assignment:  Scrape a CSV file, reformat some parts of it,
# and use an API to fill in missing parts

# Brief Description of what this module does: Connects to the zipcodebase website using your API
# key to fetch a zip code when given a city name

# Citations: https://gemini.google.com/app
# Anything else that's relevant: YOU MUST OPEN THE "Data" FOLDER IN FILE EXPLORER TO SEE THE NEW CSVs


import requests
import logging
from typing import Optional # Import Optional

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ZipLookup:
    """
    Handles interactions with the Zipcodebase API to find missing zip codes.
    """
    API_KEY = "d3d10fe0-1606-11f0-b13f-f5c8b12a9d95"
    BASE_URL = "https://app.zipcodebase.com/api/v1/code/city"

    # Updated type hint using Optional (equivalent to Union[str, None])
    def get_zip_for_city(self, city: str, country: str = 'us') -> Optional[str]:
        """
        Looks up a zip code for a given city using the Zipcodebase API.

        Args:
            city: The name of the city.
            country: The country code (defaults to 'us').

        Returns:
            A zip code string if found, otherwise None. Returns None on API error.
        """
        if not self.API_KEY:
            logging.error("API Key is missing. Cannot perform zip code lookup.")
            return None

        params = {
            'apikey': self.API_KEY,
            'city': city,
            'country': country
        }
        headers = {"apikey": self.API_KEY}

        response = None # Initialize response to None
        try:
            response = requests.get(self.BASE_URL, params=params, headers=headers, timeout=10)
            response.raise_for_status()

            data = response.json()

            if data.get("results") and isinstance(data["results"], list) and len(data["results"]) > 0:
                zip_code = data["results"][0]
                logging.info(f"Zip code found for {city}: {zip_code}")
                return str(zip_code) # Ensure it's a string
            elif data.get("results") and isinstance(data["results"], dict):
                 potential_keys = list(data["results"].keys())
                 if potential_keys:
                     first_key = potential_keys[0]
                     if isinstance(data["results"][first_key], list) and data["results"][first_key]:
                        zip_code_info = data["results"][first_key][0] # Get the dict
                        # Try common keys for zip codes within the nested dict
                        zip_code = zip_code_info.get('postal_code') or zip_code_info.get('zip_code')
                        if zip_code:
                            logging.info(f"Zip code found for {city} (dict format): {zip_code}")
                            return str(zip_code) # Ensure it's a string

            logging.warning(f"No zip code found for city: {city}. API Response: {data}")
            return None

        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for city {city}: {e}")
            if response is not None:
                 # Check response status code even on request exceptions if available
                 logging.error(f"Response status code: {response.status_code}, Response text: {response.text}")
                 if response.status_code in [401, 403]: # Check common auth error codes
                     logging.error("Check if the API key is valid or has remaining uses.")
            return None
        except Exception as e:
            logging.error(f"An unexpected error occurred during zip lookup for {city}: {e}")
            return None