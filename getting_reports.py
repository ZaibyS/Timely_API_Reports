import requests
import json

# Define the endpoint URL
url = "https://api.timelyapp.com/1.1/918926/reports/filter"

# Define the headers
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Authorization": "Bearer FfMh-1CGi4aWsu376jpMUOX6R1GVQbAGHxAY6y-eBMM"
}

# Define the request parameters
data = {
    "since": "2024-02-26",  # Start Date
    "until": "2024-02-26",  # End Date
    "group_by": ["clients", "users", "labels", "days"],
    "scope": "events"
}

# Make the POST request
response = requests.post(url, headers=headers, data=json.dumps(data))

# Check if the request was successful
if response.status_code == 200:
    # Convert the response to JSON
    reports = response.json()
    # Specify the path where you want to save the JSON file
    file_path = 'reports_02_26.json'

    # Save the JSON data to a file
    with open(file_path, 'w') as json_file:
        json.dump(reports, json_file)
    # print("Reports fetched successfully:", reports)
else:
    print(f"Failed to fetch reports. Status code: {response.status_code}")