import os
import requests
import json

def fetch_and_save_reports(since, until):

    data_folder = 'data'
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Create a folder with the specified date if it doesn't exist
    date_folder = os.path.join(data_folder, since)
    if not os.path.exists(date_folder):
        os.makedirs(date_folder)

    # Define the endpoint URL
    url = "https://api.timelyapp.com/1.1/918926/reports/filter"

    # Define the headers
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer FfMh-1CGi4aWsu376jpMUOX6R1GVQbAGHxAY6y-eBMM"
    }

    # Define the request parameters for the first API request
    data = {
        "since": since,
        "until": until,
    }

    # Make the POST request for the first API request
    response = requests.post(url, headers=headers, data=json.dumps(data))

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the response to JSON
        reports = response.json()

        # Initialize a list to store client_id and project_id pairs
        client_project_pairs = []

        # Iterate over the clients and projects in the response
        for client in reports.get("clients", []):
            client_id = client.get("id")
            client_name = client.get("name")

            for project in client.get("projects", []):
                project_id = project.get("id")
                project_name = project.get("name")

                # Add client_id and project_id pair to the list
                client_project_pairs.append((client_id,project_id))

        # Iterate over client_id and project_id pairs
        for project_id in client_project_pairs:
            # Make the API request with client_id and project_id
            data_second_request = {
                "since": since,
                "until": until,
                "project_ids": [project_id],
                "group_by": ["clients", "users", "labels", "days"],
                "scope": "events"
            }

            response_second_request = requests.post(url, headers=headers, data=json.dumps(data_second_request))

            if response_second_request.status_code == 200:
                reports_second_request = response_second_request.json()

                # Specify the path where you want to save the JSON file for this client_id and project_id pair
                file_path_second_request = os.path.join(date_folder, f'reports_{since}_{until}_{client_id}_{project_id}.json')

                # Save the JSON data to a file for this client_id and project_id pair
                with open(file_path_second_request, 'w') as json_file_second_request:
                    json.dump(reports_second_request, json_file_second_request)
            else:
                print(f"Failed to fetch reports for client_id '{client_id}', project_id '{project_id}'. "
                      f"Status code: {response_second_request.status_code}")

        # Specify the path where you want to save the JSON file for the first request
        file_path_first_request = os.path.join(date_folder, f'reports_{since}_{until}_all.json')

        # Save the JSON data to a file for the first request
        with open(file_path_first_request, 'w') as json_file_first_request:
            json.dump(reports, json_file_first_request)
    else:
        print(f"Failed to fetch reports. Status code: {response.status_code}")


fetch_and_save_reports("2024-02-26", "2024-02-26")