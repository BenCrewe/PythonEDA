import pandas as pd
import os
import shutil
from datetime import datetime

def Clean_Customers(filename, 
                    raw_folder = 'Raw', 
                    cleaned_folder = 'Cleaned', 
                    historic_folder = 'Historic',
                    timestamp_historic = True
                    ):

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
        historic_filename = f"{base}_timestamp{ext}"
    else:
        historic_filename = filename
    
    historic_path = os.path.join(historic_folder, historic_filename)

    Customers = pd.read_csv(raw_path)
    
    Customers = Customers.dropna()
    Customers = Customers[Customers['Customer ID'] %1 == 0]
    Customers['Customer ID'] = Customers['Customer ID'].astype(int)

    Customers.to_csv(cleaned_path, index = False)
    shutil.move(raw_path, historic_path)

    print("Cleaned to {cleaned_path}")
    print("Historic to {historic_path}")
    
    return

Clean_Customers("03_Library SystemCustomers.csv")
