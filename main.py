import pandas as pd
from Cleaning import Clean_Customers, Clean_System 

def run_pipeline():
    Clean_Customers('03_Library SystemCustomers.csv')
    Clean_System('03_Library Systembook.csv')


if __name__ == "__main__":
    run_pipeline()