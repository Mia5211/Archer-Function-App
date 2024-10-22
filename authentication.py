import requests
import json
import urllib3
import xml.etree.ElementTree as ET
from config import VAULT_URL,ARCHER_PROD_URL,AZURE_STORAGE_ACCOUNT_URL,CONTAINER_NAME,DIRECTORY #ARCHER_UAT_URL,
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Authenticator:
    def __init__(self):
        self.vault_url = VAULT_URL
        #self.archer_uat_url = ARCHER_UAT_URL
        self.archer_prod_url = ARCHER_PROD_URL
        self.azure_client = self._initialize_azure_client()

    def _initialize_azure_client(self):
        """Initialize Azure client."""
        credential = DefaultAzureCredential() 
        return SecretClient(vault_url=self.vault_url, credential=credential, connection_verify=False)

    def _load_template(self, template_path):
        """Load and return content of a given template."""
        with open(template_path, 'r') as file:
            return file.read()

    def archer_auth_token(self, archer_url):
        password_key = "archer-dev-password" if 'uat' in archer_url else "archer-prod-password"
        instance_key = "archer-dev-instance" if 'uat' in archer_url else "archer-prod-instance"
        
        password = self.azure_client.get_secret(password_key).value 
        instance_id = self.azure_client.get_secret(instance_key).value
        username = self.azure_client.get_secret("archer-username").value

        xml_data = self._load_template('templates/xml/create_user_session.xml').format(
            username=username, instance_id=instance_id, password=password
        )
        headers = json.loads(self._load_template('templates/headers/user_session.json'))

        response = requests.post(archer_url + "/ws/general.asmx", data=xml_data, headers=headers, verify=False)

        if response.status_code == 200:
            return ET.fromstring(response.text).find(".//{http://archer-tech.com/webservices/}CreateUserSessionFromInstanceResult").text
        else:
            return f"Error: {response.status_code}"
    
    def get_datalake_service_client():
        """Get a DataLakeServiceClient using DefaultAzureCredential and environment variables"""
        account_url = AZURE_STORAGE_ACCOUNT_URL
        credential = DefaultAzureCredential()
        return DataLakeServiceClient(account_url, credential=credential)
    
    def upload_file_to_datalake(service_client, file_content, file_name):
        """ Upload a file to Azure Data Lake Storage using environment variables for configuration and a dynamic file name."""
        file_system_name = CONTAINER_NAME
        base_path = DIRECTORY
        # Combine base path with the dynamic file name
        full_path = f"{base_path}/{file_name}"
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        file_client = file_system_client.get_file_client(full_path)
        file_client.upload_data(file_content, overwrite=True)
        return full_path  # Return the full path for confirmation