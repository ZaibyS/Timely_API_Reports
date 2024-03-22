from extract_data import fetch_and_save_reports
from transform_data import transform
# from load_data import load
from datetime import datetime, timedelta
from combined_reports import combine_csv_files
import concurrent.futures



def fetch_transform_and_load(date, connection_string):

    print(f"Date: {date}")
    # print("Starting Extracting Data")
    # Fetching the data from the API
    fetch_and_save_reports(date)
    # print("Starting Transforming Data")
    # Tranforming the data
    transform(date)
    # print("Starting Loading Data")
    # #Load the Data in DB
    # load(date,connection_string)
    print(f"Data for {date} fetched, transformed")

if __name__ == "__main__":
    connection_string = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:ivc-consulting-sql-server.database.windows.net,1433;Database=ivc_consulting_db;Uid=ivc-consulting;Pwd=hN3$Kp#9@Lm7;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    start_date = datetime.strptime("2024-02-01", "%Y-%m-%d")
    end_date = datetime.strptime("2024-03-15", "%Y-%m-%d")

    num_threads = 10
        
    # Create a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit tasks for each date
        futures = {
            executor.submit(
                fetch_transform_and_load, 
                current_date.strftime("%Y-%m-%d"), connection_string
                ): 
                current_date for current_date in (start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1))
                }
        
        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            date = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Error occurred while processing data for {date}: {exc}")
    
    combine_csv_files('reports', 'combined_report/combined_report.csv')
    # load(connection_string)