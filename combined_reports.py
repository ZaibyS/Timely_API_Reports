import os

def combine_csv_files(input_folder, output_csv):
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    # Check if there are any CSV files
    if not csv_files:
        print("No CSV files found in the folder.")
        return
    
    # Create directory if it does not exist
    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    
    # Open output CSV file for writing
    with open(output_csv, mode='w', newline='', encoding='utf-8') as output_file:
        # Iterate over CSV files
        for csv_file in csv_files:
            csv_path = os.path.join(input_folder, csv_file)
            # Open each CSV file and append its content to the output CSV file
            with open(csv_path, mode='r', encoding='utf-8') as input_csv:
                # Skip header if not the first CSV file
                if csv_file != csv_files[0]:
                    next(input_csv)
                # Append non-empty rows to output CSV file
                for line in input_csv:
                    if line.strip():  # Check if line is not empty
                        output_file.write(line)
    
    print("Combined CSV files into", output_csv)

# combine_csv_files('reports', 'combined_report/combined_report.csv')
    