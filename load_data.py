import pyodbc
import pandas as pd
import csv

def load_data(data, connection_string):
    try:
        conn = pyodbc.connect(connection_string)

        cursor = conn.cursor()

        for row in data:
            cursor.execute(
                f"""INSERT INTO timely.timely_extract (client, project, hour_date, name, billable_hours, logged_money, hour_tags,
                    hour_billed_status, hour_note, hour_from, hour_to, teams, external_id, logged_hour, non_billable_hours,
                    budget_type, budget_interval, budget_total, budget_spent, budget_spent_percenatge, budget_remaining,
                    budget_remaining_percentage, logged_cost, planned_hours, planned_money, planned_cost, email, project_description)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    row.get("Client", ""),
                    row.get("Project", ""),
                    row.get("Hour Date", ""),
                    row.get("Name", ""),
                    row.get("Billable Hours", ""),
                    row.get("Logged Money", ""),
                    row.get("Hour Tags", ""),
                    row.get("Hour Billed Status", ""),
                    row.get("Hour Note", ""),
                    row.get("Hour From", ""),
                    row.get("Hour To", ""),
                    row.get("Teams", ""),
                    row.get("External ID", ""),
                    row.get("Logged Hours", ""),
                    row.get("Non-billable Hours", ""),
                    row.get("Budget Type", ""),
                    row.get("Budget Interval", ""),
                    row.get("Budget Total", ""),
                    row.get("Budget Spent", ""),
                    row.get("Budget Spent (%)", ""),
                    row.get("Budget Remaining", ""),
                    row.get("Budget Remaining (%)", ""),
                    row.get("Logged Cost", ""),
                    row.get("Planned Hours", ""),
                    row.get("Planned Money", ""),
                    row.get("Planned Cost", ""),
                    row.get("Email", ""),
                    row.get("Project Description", ""),
                )    
            )

        conn.commit()

        print("Data inserted successfully into SQL Server")

    except Exception as e:
        print("Error occurred while inserting data into SQL Server:", str(e))

    finally:
        cursor.close()
        conn.close()

def read_csv(file_path, encoding='utf-8'):
    data = []
    with open(file_path, 'r', newline='', encoding=encoding) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data

def load(date,connection_string):
    df = read_csv(f'reports/extracted_{date}.csv')
    load_data(df, connection_string)

# connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ivc-consulting-sql-server.database.windows.net,1433;Database=ivc_consulting_db;Uid=ivc-consulting;Pwd=hN3$Kp#9@Lm7;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
# date = "2024-02-05"
# load(date,connection_string)