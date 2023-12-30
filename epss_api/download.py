import requests
from concurrent.futures import ThreadPoolExecutor
import csv
import time
import pandas as pd
import numpy as np
from pandas import json_normalize
import json
from collections.abc import Iterable
from alive_progress import alive_bar

url = "https://api.first.org/data/v1/epss?percentile-gt=0.95&epss-gt=0.95&pretty=true"
max_offset = 160000
max_threads = 3  # Adjust as needed
current_offset = 0
offset_increment = 100

def flatten(lis):
    flatList = []
    # Iterate with outer list
    for element in lis:
        if type(element) is list:
            # Check if type is list than iterate through the sublist
            for item in element:
                flatList.append(item)
        else:
            flatList.append(element)
    return flatList
             
def fetch_data(offset):
    params = {
        "envelope": "true",
        "pretty": "true",
        "offset": offset
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        json_data = response.json()
        data = json_data.get("data",[])
        total = json_data.get("total", [0])
        remoteOffset = json_data.get("offset", [0])
        if int(total) <= int(offset):
            print("---=== Offset reached ===---")
            return None, remoteOffset
        if not data:  # Check if data is empty
            return None
        return data, total
    else:
        print(f"Request for offset {offset} failed with status code: {response.status_code}")
        return None


start_time = time.time()
results = []

with ThreadPoolExecutor(max_workers=max_threads) as executor:
    while current_offset <= max_offset:
        print("Gettting the data for:",current_offset)
        data, total = fetch_data(current_offset)
        with alive_bar(total, title="Getting EPSS Data...", ctrl_c=False) as bar:
            
            if data:
                #token = json.dumps(data[0])
                #token = token.replace('\'','\"')
                results.append(data[0])
                current_offset += offset_increment 
                bar(current_offset)
                # Update the offset using the total count from JSON
            else:
                print("Empty data received or failed request. Stopping iteration.")
                break  # Stop the iteration if empty data is received or request fails

# Writing only the 'data' portion to a CSV file
with open('responses.json', 'w', encoding='utf-8') as csvfile:
    json.dump(flatten(results),csvfile, indent=4, sort_keys=True)
    
end_time = time.time()
elapsed_time = end_time - start_time

estimated_time_per_request = elapsed_time / (max_offset + 1)
total_estimated_time = estimated_time_per_request * max_offset

print(f"Estimated time per request: {estimated_time_per_request:.4f} seconds")
print(f"Total estimated time for {max_offset + 1} requests: {total_estimated_time:.2f} seconds")
end_time = time.time()
elapsed_time = end_time - start_time

estimated_time_per_request = elapsed_time / (max_offset + 1)
total_estimated_time = estimated_time_per_request * max_offset

print(f"Estimated time per request: {estimated_time_per_request:.4f} seconds")
print(f"Total estimated time for {max_offset + 1} requests: {total_estimated_time:.2f} seconds")

