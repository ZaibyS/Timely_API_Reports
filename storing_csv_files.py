import json
import csv

def extract_and_store_csv(json_filename, csv_filename):
    # Read JSON data from file
    with open(json_filename, 'r') as json_file:
        data = json.load(json_file)

    # Extracted data storage
    extracted_data = []

    # Iterate through each JSON object in the list
    for entry in data:
        Client = entry['project']['client']['name']
        Client_ID = entry['project']['client']['id']
        Project = entry['project']['name']
        Project_ID = entry['project']['id']
        Hour_Date = entry['day']
        user_name = entry['user']['name']
        user_id = entry['user']['id']
        user_email = entry['user']['email']
        billable_hours = entry['duration']['total_hours']
        logged_money = entry['cost']['amount']
        hour_billed_status = entry['billed']
        hour_notes = entry['note']
        external_id = entry['external_link_ids']
        budget_type = entry['project']['budget_type']
        project_description = entry['project']['description']

        # Extract timestamps dynamically
        timestamps = entry.get('timestamps', [])
        for timestamp in timestamps:
            from_time = timestamp['from'][11:16]  # Extracts only the hour and minute
            to_time = timestamp['to'][11:16]  # Extracts only the hour and minute

            # Append the extracted data to the list
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
                'Hour Tags': None,
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
        
        # If no timestamps, add a row with 'From' and 'To' as None
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
                'Hour Tags': None,
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

    # Write the extracted data to a CSV file
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Client', 'Client ID', 'Project','Project ID', 'Hour Date', 'Name', 'ID', 'Billable Hours', 'Logged Money', 'Hour Tags',
                'Hour Billed Status', 'Hour Note', 'Hour From', 'Hour To', 'Teams', 'External ID',
                'Logged Hours', 'Non-billable Hours', 'Budget Type', 'Budget Interval', 'Budget Total',
                'Budget Spent', 'Budget Spent (%)', 'Budget Remaining', 'Budget Remaining (%)', 'Logged Cost',
                'Planned Hours', 'Planned Money (€)', 'Planned Cost (€)', 'Email', 'Project Description']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data
        writer.writerows(extracted_data)

# Example usage:
json_filename = 'reports_28_02.json'
csv_filename = 'extracted_data_28_02.csv'
extract_and_store_csv(json_filename, csv_filename)