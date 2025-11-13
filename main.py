import pandas as pd
from Functions.Cleaning import Clean_Customers, Clean_System, merge_metrics

def run_pipeline():
    customer_metrics = Clean_Customers('03_Library SystemCustomers.csv')
    system_metrics = Clean_System('03_Library Systembook.csv')
    
    metrics_df = merge_metrics(customer_metrics, system_metrics)

    return metrics_df


if __name__ == "__main__":
    run_pipeline()

