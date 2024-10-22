import logging
import datetime
import azure.functions as func
import pandas as pd
import numpy as np
from archer_api import ArcherAPI
from authentication import Authenticator
from config import INCIDENT_REPORT_TOKEN


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    
    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    logging.info('Starting main function...')
    
    # Instantiate the classes
    auth = Authenticator()
    archer_api = ArcherAPI()
    logging.info('Classes instantiated.')
    
    # Authenticate and get required data
    try:
        archer_token = auth.archer_auth_token(auth.archer_prod_url)
        logging.info("Acquired Archer token.")
    except Exception as e:
        logging.error(f"Error acquiring Archer token: {e}")
        return
    
    try:
        archer_incident_report_guid = auth.azure_client.get_secret("archer-prod-incident-report-guid").value
        url = auth.azure_client.get_secret("archer-search-url").value
        logging.info("Fetched secrets from Azure.")
    except Exception as e:
        logging.error(f"Error fetching secrets from Azure: {e}")
        return
    
    # Fetch the data
    try:
        df = archer_api.fetch_all_report_pages(archer_token, INCIDENT_REPORT_TOKEN, url)
        logging.info("Data fetched from Archer API.")
    except Exception as e:
        logging.error(f"Error fetching data from Archer API: {e}")
        return
    
    # Identify columns with the word "Date" in their headers
    date_columns = [col for col in df.columns if 'Date' in col]
    logging.info(f"Identified date columns: {date_columns}")
    
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.normalize()
    logging.info("Converted date columns to datetime format.")
    
    today = pd.Timestamp('now').normalize()
    df['Days Open'] = np.where(pd.isnull(df['Date/Time Closed']),
                               (today - df['Date Created']).dt.days,
                               (df['Date/Time Closed'] - df['Date Created']).dt.days)
    logging.info("Calculated 'Days Open' column.")
    
    file_name = 'incident_report_alicja_report.parquet'
    
    # Upload to Azure Blob Storage
    try:
        # Convert dataframe to parquet
        parquet_data = df.to_parquet()
        
        # Get DataLakeServiceClient using authentication library (which uses DefaultAzureCredential)
        service_client = Authenticator.get_datalake_service_client()
        
        # Upload file using authentication library
        full_path = Authenticator.upload_file_to_datalake(service_client, parquet_data, file_name)
        
        logging.info(f"File {file_name} uploaded successfully to Azure Blob Storage container at path:{full_path}")
    except Exception as e:
        logging.error(f"Error uploading file to Azure Blob Storage: {e}")