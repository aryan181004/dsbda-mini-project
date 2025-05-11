import pandas as pd
import json
import os

def process_json_files(input_dir, output_dir):
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process JSON files
    for filename in os.listdir(input_dir):
        if filename.endswith('.json'):
            with open(os.path.join(input_dir, filename), 'r') as f:
                data = json.load(f)
            
            # Create DataFrame from JSON
            df = pd.json_normalize(data['items'])
            
            # Save as parquet file
            output_file = os.path.join(output_dir, f"{filename.split('.')[0]}.parquet")
            df.to_parquet(output_file)

def process_csv_files(input_dir, output_dir):
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Process CSV files
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            try:
                # First try UTF-8
                df = pd.read_csv(os.path.join(input_dir, filename))
            except UnicodeDecodeError:
                # If UTF-8 fails, try with different encoding
                df = pd.read_csv(os.path.join(input_dir, filename), encoding='latin1')
            
            # Extract region from filename (e.g., 'USvideos.csv' -> 'US')
            region = filename.split('videos.csv')[0]
            
            # Save as parquet file
            output_file = os.path.join(output_dir, f"{region}_processed.parquet")
            df.to_parquet(output_file)

def main():
    # Define directories
    raw_dir = "dataset"
    processed_dir = "processed_data"
    
    # Process both JSON and CSV files
    process_json_files(raw_dir, processed_dir)
    process_csv_files(raw_dir, processed_dir)
    
    print("Data processing completed!")

if __name__ == "__main__":
    main()