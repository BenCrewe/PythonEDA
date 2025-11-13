import pandas as pd
import os
import shutil
from datetime import datetime
# from ingestion import Engine
from sqlalchemy import create_engine
from datetime import datetime
import time

def Clean_Customers(filename, 
                    raw_folder = 'Raw', 
                    cleaned_folder = 'Cleaned', 
                    historic_folder = 'Historic',
                    timestamp_historic = True
                    ):
    start_time = datetime.now()
    t0 = time.time()

    # server = 'localhost'
    # database = 'library'
    # driver = 'ODBC Driver 17 for SQL Server'

    # connection_string = f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&Driver=ODBC+Driver+17+for+SQL+Server'

    # engine = create_engine(connection_string)

    # Ensure Folders Exist
    os.makedirs(raw_folder, exist_ok=True)
    os.makedirs(cleaned_folder, exist_ok=True)
    os.makedirs(historic_folder, exist_ok=True)


    # Define file paths
    raw_path = os.path.join(raw_folder,filename)
    cleaned_path = os.path.join(cleaned_folder, filename)

    if timestamp_historic:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base, ext = os.path.splitext(filename)
        historic_filename = f"{base}_{timestamp}{ext}"
    else:
        historic_filename = filename
    
    historic_path = os.path.join(historic_folder, historic_filename)

    Customers = pd.read_csv(raw_path)
    initial_rows = len(Customers)
    
    Customers = Customers.dropna()
    Customers = Customers[Customers['Customer ID'] %1 == 0]
    Customers['Customer ID'] = Customers['Customer ID'].astype(int)

    rows_dropped = initial_rows - len(Customers)

    # Customers.to_sql('Customers', con=engine, if_exists='replace', index=False)
    Customers.to_csv(cleaned_path, index = False)
    shutil.move(raw_path, historic_path)

    print(f"Customers Cleaned to {cleaned_path}")
    print(f"Customers Historic to {historic_path}")

    duration = time.time() - t0
    end_time = datetime.now()
    duration = duration * 100

    print("Customer Cleaning... sweep sweep")
    print("Start time: ",start_time)
    print("End time: ",end_time)
    print("Duration:", duration)
    print("Initial Rows: ",initial_rows)
    print("Rows Dropped: ", rows_dropped)

    metrics_df = pd.DataFrame([{
        'Stage': 'Clean_System',
        'Start_Time': start_time,
        'End_Time': end_time,
        'Duration': duration,
        'Initial_Rows': initial_rows,
        'Rows_Dropped': rows_dropped
    }])
    print(metrics_df)
    return metrics_df



def Clean_System(filename):
    start_time = datetime.now()
    t0 = time.time()

    # server = 'localhost'
    # database = 'library'
    # driver = 'ODBC Driver 17 for SQL Server'

    # connection_string = f'mssql+pyodbc://@{server}/{database}?trusted_connection=yes&Driver=ODBC+Driver+17+for+SQL+Server'

    # engine = create_engine(connection_string)

    raw_folder = 'Raw'
    cleaned_folder = 'Cleaned'
    historic_folder = 'Historic'
    timestamp_historic = True

    raw_path = os.path.join(raw_folder,filename)
    cleaned_path = os.path.join(cleaned_folder, filename)

    if timestamp_historic:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base, ext = os.path.splitext(filename)
        historic_filename = f"{base}_{timestamp}{ext}"
    else:
        historic_filename = filename
    
    historic_path = os.path.join(historic_folder, historic_filename)

    raw_path = os.path.join(raw_folder,filename)
    
    System = pd.read_csv(raw_path)
    initial_rows = len(System)
    System = System.dropna()
    System = System[System['Id'] %1 == 0]
    System['Id'] = System['Id'].astype(int)
    System['Book checkout'] = System['Book checkout'].str.replace('"','',regex=True)
    System['Book checkout'] = pd.to_datetime(System['Book checkout'], format='mixed', errors='coerce')
    System = System.dropna(subset=['Book checkout'])
    System['Book Returned'] = System['Book Returned'].str.replace('"','',regex=True)
    System['Book Returned'] = pd.to_datetime(System['Book Returned'], format='mixed', errors='coerce')
    System = System.dropna(subset=['Book Returned'])
    System['Rental_Length'] = System['Book Returned'] - System['Book checkout']

    rows_dropped = initial_rows - len(System)

    # System.to_sql('System', con=engine, if_exists='replace', index=False)
    System.to_csv(cleaned_path, index = False)
    shutil.move(raw_path, historic_path)

    print(f"System Cleaned to {cleaned_path}")
    print(f"System Historic to {historic_path}")

    duration = time.time() - t0
    duration = duration * 100
    end_time = datetime.now()

    print("System Cleaning... sweep sweep")
    print("Customer Cleaning... sweep sweep")
    print("Start time: ", start_time)
    print("End time: ", end_time)
    print("Duration:", duration)
    print("Initial Rows: ", initial_rows)
    print("Rows Dropped: ", rows_dropped)

    metrics_df = pd.DataFrame([{
        'Stage': 'Customer_System',
        'Start_Time': start_time,
        'End_Time': end_time,
        'Duration': duration,
        'Initial_Rows': initial_rows,
        'Rows_Dropped': rows_dropped
    }])
    print(metrics_df)
    return metrics_df
        
def merge_metrics(*metrics_df, output_file='pipeline_metrics.csv'):
    all_metrics = pd.concat(metrics_df, ignore_index=True)
    all_metrics.to_csv('log/Pipe_Metrics.csv', index = False)
    return all_metrics