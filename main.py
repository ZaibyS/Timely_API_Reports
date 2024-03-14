from extract_data import fetch_and_save_reports
from transform_data import transform
from load_data import load
from datetime import datetime, timedelta



def fetch_and_load(date, connection_string):

    print(f"Date: {date}")
    print("Starting Extracting Data")
    # Fetching the data from the API
    fetch_and_save_reports(date)
    print("Starting Transforming Data")
    # Tranforming the data
    transform(date)
    print("Starting Loading Data")
    #Load the Data in DB
    load(date,connection_string)

if __name__ == "__main__":
    connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ivc-consulting-sql-server.database.windows.net,1433;Database=ivc_consulting_db;Uid=ivc-consulting;Pwd=hN3$Kp#9@Lm7;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    start_date = datetime.strptime("2024-02-06", "%Y-%m-%d")
    end_date = datetime.strptime("2024-03-13", "%Y-%m-%d")
    current_date = start_date
    while current_date <= end_date:
        fetch_and_load(current_date.strftime("%Y-%m-%d"), connection_string)
        current_date += timedelta(days=1)
