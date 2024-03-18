import os
import requests
import json
import pandas as pd
import pyodbc
from datetime import datetime, timedelta

def extract_label_name(label_id, labels_data):
    for label in labels_data:
        if label['id'] == label_id:
            return label['name']
    return None

def transform(json_data, json_label_data):
    labels_data = json_label_data['labels']
    
    extracted_data = []

    fieldnames = ['Client', 'Project', 'Hour Date', 'Name', 'Billable Hours', 'Logged Money',
                  'Hour Tags', 'Hour Billed Status', 'Hour Note', 'Hour From', 'Hour To', 'Teams', 'External ID',
                  'Logged Hours', 'Non-billable Hours', 'Budget Type', 'Budget Interval', 'Budget Total',
                  'Budget Spent', 'Budget Spent (%)', 'Budget Remaining', 'Budget Remaining (%)', 'Logged Cost',
                  'Planned Hours', 'Planned Money', 'Planned Cost', 'Email', 'Project Description']

    for entry in json_data:
        user_name = entry['user']['name']

        # Skip rows with specified users
        if user_name in ['Deimante Altmanaite', 'Mertcan Ozhabes', 'Lina Laurynaite']:
            continue

        Client = entry['project']['client']['name']
        Client_ID = entry['project']['client']['id']
        Project = entry['project']['name']
        Project_ID = entry['project']['id']
        Hour_Date = entry['day']
        user_id = entry['user']['id']
        user_email = entry['user']['email']
        billable_hours = round(entry['duration']['total_hours'], 2)
        logged_hours = round(entry['duration']['total_hours'], 2)
        logged_money = entry['cost']['amount']
        hour_billed_status = "Yes" if entry['billed'] else "No"
        hour_notes = entry['note']
        external_id = ','.join(map(str, entry['external_link_ids']))
        budget_type = entry['project']['budget_type']
        project_description = entry['project']['description']
        estimated_duration = round(entry['estimated_duration']['total_hours'], 2)
        estimated_cost = entry['estimated_cost']['amount']

        # Extract label_ids and required_label_ids
        label_ids = entry['label_ids']
        required_label_ids = entry['project']['required_label_ids']

        # Calculate the difference between label_ids and required_label_ids
        label_id_difference = list(set(label_ids) - set(required_label_ids))

        # Fetch label names based on label IDs in 'Label ID Difference'
        label_names = [extract_label_name(int(label_id), labels_data) for label_id in label_id_difference]

        # Convert label names to a comma-separated string
        label_names_str = ','.join(filter(None, label_names))

        if logged_money == 0:
            non_billable_hours = billable_hours
            billable_hours = 0
            logged_money = None
        else:
            non_billable_hours = 0

        # Extract timestamps dynamically
        timestamps = entry.get('timestamps', [])

        if timestamps:
            first_timestamp = timestamps[0]
            from_time = first_timestamp['from'][11:16]  # Extracts only the hour and minute
            to_time = first_timestamp['to'][11:16]  # Extracts only the hour and minute
        else:
            from_time = None
            to_time = None

        extracted_data.append({
            'Client': Client,
            'Project': Project,
            'Hour Date': Hour_Date,
            'Name': user_name,
            'Billable Hours': billable_hours,
            'Logged Money': logged_money,
            'Hour Tags': label_names_str,
            'Hour Billed Status': hour_billed_status,
            'Hour Note': hour_notes,
            'Hour From': from_time,
            'Hour To': to_time,
            'Teams': None,
            'External ID': external_id,
            'Logged Hours': logged_hours,
            'Non-billable Hours': non_billable_hours,
            'Budget Type': budget_type,
            'Budget Interval': None,
            'Budget Total': None,
            'Budget Spent': None,
            'Budget Spent (%)': None,
            'Budget Remaining': None,
            'Budget Remaining (%)': None,
            'Logged Cost': None,
            'Planned Hours': estimated_duration,
            'Planned Money': estimated_cost,
            'Planned Cost': None,
            'Email': user_email,
            'Project Description': project_description
        })

    df = pd.DataFrame(extracted_data, columns=fieldnames)

    return df

def fetch_and_save_reports(date):

    since = date.isoformat()
    until = (date + timedelta(days=1)).isoformat() 
    all_reports_df = pd.DataFrame()

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
                transformed_data = transform(reports_second_request, reports)
                all_reports_df = pd.concat([all_reports_df, transformed_data], ignore_index=True)
            else:
                print(f"Failed to fetch reports for client_id '{client_id}', project_id '{project_id}'. "
                      f"Status code: {response_second_request.status_code}")
    else:
        print(f"Failed to fetch reports. Status code: {response.status_code}")
    
    return all_reports_df

def load_data(data, connection_string):
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        for index, row in data.iterrows():
            # Replace NaN values with None before insertion
            row.fillna('', inplace=True)

            try:
                cursor.execute(
                    """INSERT INTO timely.timely_extract (client, project, hour_date, name, billable_hours, logged_money, hour_tags,
                        hour_billed_status, hour_note, hour_from, hour_to, teams, external_id, logged_hour, non_billable_hours,
                        budget_type, budget_interval, budget_total, budget_spent, budget_spent_percenatge, budget_remaining,
                        budget_remaining_percentage, logged_cost, planned_hours, planned_money, planned_cost, email, project_description)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["Client"],
                        row["Project"],
                        row["Hour Date"],
                        row["Name"],
                        row["Billable Hours"],
                        row["Logged Money"],
                        row["Hour Tags"],
                        row["Hour Billed Status"],
                        row["Hour Note"],
                        row["Hour From"],
                        row["Hour To"],
                        row["Teams"],
                        row["External ID"],
                        row["Logged Hours"],
                        row["Non-billable Hours"],
                        row["Budget Type"],
                        row["Budget Interval"],
                        row["Budget Total"],
                        row["Budget Spent"],
                        row["Budget Spent (%)"],
                        row["Budget Remaining"],
                        row["Budget Remaining (%)"],
                        row["Logged Cost"],
                        row["Planned Hours"],
                        row["Planned Money"],
                        row["Planned Cost"],
                        row["Email"],
                        row["Project Description"],
                    )
                )

            except Exception as e:
                print(f"Error occurred while inserting row {index} into SQL Server:", str(e))
                print("Problematic row:", row)

        conn.commit()
        print("Data inserted successfully into SQL Server")

    except Exception as e:
        print("Error occurred while inserting data into SQL Server:", str(e))

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ivc-consulting-sql-server.database.windows.net,1433;Database=ivc_consulting_db;Uid=ivc-consulting;Pwd=hN3$Kp#9@Lm7;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    date = datetime.strptime("2024-03-15", "%Y-%m-%d")

    print("Extracting Data")
    df = fetch_and_save_reports(date)
    print("Loading Data")
    load_data(df, connection_string)
    print("Finished Loading Data")
