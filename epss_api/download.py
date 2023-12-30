import requests
import json
import threading
from alive_progress import alive_bar

BASE_URL = "https://api.first.org/data/v1/epss"
OFFSET_INCREMENT = 100
THREAD_COUNT = 3
LOCK = threading.Lock()

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

def fetch_data(offset):
    url = f"{BASE_URL}?percentile-gt=0.95&epss-gt=0.95&pretty=true&offset={offset}"
    response = requests.get(url)
    data = response.json()
    return data

def process_data(offset, bar):
    data = fetch_data(offset)
    while data.get('data'):
        with LOCK:
            all_results.extend(data['data'])
            offset += (OFFSET_INCREMENT)
            bar()
        data = fetch_data(offset)

def main():
    global all_results
    all_results = []
    threads = []

    with alive_bar(0, bar='blocks') as bar:
        for i in range(THREAD_COUNT):
            thread = threading.Thread(target=process_data, args=(i * OFFSET_INCREMENT, bar))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    json_object = json.dumps(all_results,indent=4, sort_keys=True)
    print("---=== json data validation is OK:",validateJSON(json_object))
    with open('responses.json', 'w') as file:
        json.dump(all_results, file, indent=2)
        print("All results saved to all_results.json")

if __name__ == "__main__":
    main()
