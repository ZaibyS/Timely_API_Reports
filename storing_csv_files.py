import os
import json
import csv

def extract_label_name(label_id, labels_data):
    for label in labels_data:
        if label['id'] == label_id:
            return label['name']
    return None

def extract_and_store_csv(json_folder, csv_filename, labels_filename):
    with open(labels_filename, 'r') as labels_file:
        labels_data = json.load(labels_file)['labels']

    extracted_data = []

    csv_exists = os.path.exists(csv_filename) and os.path.getsize(csv_filename) > 0

    with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Client', 'Client ID', 'Project', 'Project ID', 'Hour Date', 'Name', 'ID', 'Billable Hours', 'Logged Money',
                      'Hour Tags', 'Hour Billed Status', 'Hour Note', 'Hour From', 'Hour To', 'Teams', 'External ID',
                      'Logged Hours', 'Non-billable Hours', 'Budget Type', 'Budget Interval', 'Budget Total',
                      'Budget Spent', 'Budget Spent (%)', 'Budget Remaining', 'Budget Remaining (%)', 'Logged Cost',
                      'Planned Hours', 'Planned Money (€)', 'Planned Cost (€)', 'Email', 'Project Description']
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not csv_exists:
            writer.writeheader()

        # Iterate through each JSON file in the specified folder
        for filename in os.listdir(json_folder):
            if filename.endswith(".json") and filename != 'reports_2024-02-26_2024-02-26_all.json':
                json_filepath = os.path.join(json_folder, filename)

                with open(json_filepath, 'r') as json_file:
                    data = json.load(json_file)

                for entry in data:
                    Client = entry['project']['client']['name']
                    Client_ID = entry['project']['client']['id']
                    Project = entry['project']['name']
                    Project_ID = entry['project']['id']
                    Hour_Date = entry['day']
                    user_name = entry['user']['name']
                    user_id = entry['user']['id']
                    user_email = entry['user']['email']
                    billable_hours = round(entry['duration']['total_hours'], 2)
                    logged_money = entry['cost']['amount']
                    hour_billed_status = "Yes" if entry['billed'] else "No"
                    hour_notes = entry['note']
                    external_id = ','.join(map(str, entry['external_link_ids']))
                    budget_type = entry['project']['budget_type']
                    project_description = entry['project']['description']

                    # Extract label_ids and required_label_ids
                    label_ids = entry['label_ids']
                    required_label_ids = entry['project']['required_label_ids']

                    # Calculate the difference between label_ids and required_label_ids
                    label_id_difference = list(set(label_ids) - set(required_label_ids))

                    # Fetch label names based on label IDs in 'Label ID Difference'
                    label_names = [extract_label_name(int(label_id), labels_data) for label_id in label_id_difference]

                    # Convert label names to a comma-separated string
                    label_names_str = ','.join(filter(None, label_names))

                    # Extract timestamps dynamically
                    timestamps = entry.get('timestamps', [])
                    for timestamp in timestamps:
                        from_time = timestamp['from'][11:16]  # Extracts only the hour and minute
                        to_time = timestamp['to'][11:16]  # Extracts only the hour and minute

                        extracted_data.append({
                            'Client': Client,
                            'Client ID' : Client_ID,
                            'Project': Project,
                            'Project ID' : Project_ID,
                            'Hour Date': Hour_Date,
                            'Name' : user_name,
                            'ID' : user_id,
                            'Billable Hours' : billable_hours,
                            'Logged Money': logged_money,
                            'Hour Tags': label_names_str,
                            'Hour Billed Status' : hour_billed_status,
                            'Hour Note': hour_notes,
                            'Hour From': from_time,
                            'Hour To': to_time,
                            'Teams': None,
                            'External ID': external_id,
                            'Logged Hours' : None,
                            'Non-billable Hours': None,
                            'Budget Type': budget_type,
                            'Budget Interval' : None,
                            'Budget Total' : None,
                            'Budget Spent' : None,
                            'Budget Spent (%)' : None,
                            'Budget Remaining' : None,
                            'Budget Remaining (%)' : None,
                            'Logged Cost': None,
                            'Planned Hours': 0,
                            'Planned Money (€)' : 0,
                            'Planned Cost (€)' : None,
                            'Email' : user_email,
                            'Project Description' : project_description
                        })

                    if not timestamps:
                        extracted_data.append({
                            'Client': Client,
                            'Client ID' : Client_ID,
                            'Project': Project,
                            'Project ID' : Project_ID,
                            'Hour Date': Hour_Date,
                            'Name' : user_name,
                            'ID' : user_id,
                            'Billable Hours' : billable_hours,
                            'Logged Money': logged_money,
                            'Hour Tags': label_names_str,
                            'Hour Billed Status' : hour_billed_status,
                            'Hour Note': hour_notes,
                            'Hour From': None,
                            'Hour To': None,
                            'Teams': None,
                            'External ID': external_id,
                            'Logged Hours' : None,
                            'Non-billable Hours': None,
                            'Budget Type': budget_type,
                            'Budget Interval' : None,
                            'Budget Total' : None,
                            'Budget Spent' : None,
                            'Budget Spent (%)' : None,
                            'Budget Remaining' : None,
                            'Budget Remaining (%)' : None,
                            'Logged Cost': None,
                            'Planned Hours': 0,
                            'Planned Money (€)' : 0,
                            'Planned Cost (€)' : None,
                            'Email' : user_email,
                            'Project Description' : project_description
                        })

        writer.writerows(extracted_data)

json_folder = 'data/2024-02-26' 
csv_filename = 'extracted_data__date.csv'
labels_filename = 'data/2024-02-26/reports_2024-02-26_2024-02-26_all.json' 
extract_and_store_csv(json_folder, csv_filename, labels_filename)

