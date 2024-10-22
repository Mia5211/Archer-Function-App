import logging
import requests
import urllib3
import xml.etree.ElementTree as ET
import pandas as pd
import json

# Disable warnings for insecure requests to improve output clarity.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ArcherAPI:

    def __init__(self):
        """Initialize the ArcherAPI class."""
        pass

    def _load_template(self, template_path):
        """
        Load and return content of a given template.
        
        Args:
            template_path (str): Path to the template file.

        Returns:
            str: Content of the template file.
        """
        # Open and read the content of the specified template file.
        with open(template_path, 'r') as file:
            return file.read()

    def _load_headers(self, header_path):
        """
        Load and return headers from a given JSON file.
        
        Args:
            header_path (str): Path to the JSON header file.

        Returns:
            dict: Dictionary containing the headers.
        """
        # Open and parse the content of the specified JSON header file.
        with open(header_path, 'r') as file:
            return json.load(file)

    def archer_incident_report_xml(self, archer_token, archer_incident_report_guid, archer_page_number):
        """
        Generate and return the XML report for Archer incidents based on the provided arguments.

        Args:
            archer_token (str): Archer token.
            archer_incident_report_guid (str): GUID of the Archer incident report.
            archer_page_number (int): Page number for paginated reports.

        Returns:
            str: XML formatted report.
        """
        # Load the XML template and format it with the provided arguments.
        xml_template = self._load_template('templates/xml/archer_incident_report.xml')
        return xml_template.format(archer_token=archer_token, 
                                   archer_incident_report_guid=archer_incident_report_guid, 
                                   archer_page_number=archer_page_number)

    def response_to_xml(self, response, request_type):
        """
        Convert the API response to XML format.

        Args:
            response (Response): API response object.
            request_type (str): Type of the request to locate the correct XML data.

        Returns:
            ET.ElementTree: Parsed XML tree.
        """
        # Convert the API response to XML, then find and return the desired section.
        find_string = ".//{http://archer-tech.com/webservices/}" + request_type
        return ET.ElementTree(ET.fromstring(ET.fromstring(response.text).find(find_string).text))

    def get_report_headers(self, xml_tree):
        """
        Extract and return report headers from the XML tree.

        Args:
            xml_tree (ET.ElementTree): XML tree containing the report data.

        Returns:
            dict: Dictionary of field IDs and their respective names.
        """
        # Create a dictionary to store field IDs and their names.
        field_definitions_dict = {}
        for field in xml_tree.findall('.//FieldDefinition'):
            field_id = field.get('id')
            field_name = field.get('name')
            field_definitions_dict[field_id] = field_name
        return field_definitions_dict

    def get_report_records(self, xml_tree, report_headers):
        """
        Extract and return records from the XML report.

        Args:
            xml_tree (ET.ElementTree): XML tree containing the report data.
            report_headers (dict): Dictionary of field IDs and their respective names.

        Returns:
            list: List of dictionaries representing each record.
        """
        def extract_list_value(field):
            # Extract the display name from ListValues, if present.
            list_value_element = field.find('.//ListValues/ListValue')
            if list_value_element is not None:
                return list_value_element.get('displayName')
            return None

        def extract_reference(field):
            # Extract the text from Reference fields, if present.
            reference_element = field.find('.//Reference')
            if reference_element is not None:
                return reference_element.text.strip() if reference_element.text else ''
            return None

        def extract_users(field):
            # Extract email, first name, and last name from User fields, if present.
            user_element = field.find('.//Users/User')
            if user_element is not None:
                user_data = {
                    'email': user_element.text.strip() if user_element.text else '',
                    'firstName': user_element.get('firstName', ''),
                    'lastName': user_element.get('lastName', '')
                }
                return user_data
            return None

        # Create a list to store the extracted records.
        records_list = []
        for record in xml_tree.findall('.//Record'):
            record_dict = {}
            for field in record.findall('.//Field'):
                field_id = field.get('id')
                field_value = field.text.strip() if field.text else ''
                field_name = report_headers.get(field_id, f'Field_{field_id}')

                # Extract data based on field type.
                if field_value == '':
                    extracted_list_value = extract_list_value(field)
                    extracted_reference = extract_reference(field)
                    extracted_user = extract_users(field)
                    
                    if extracted_list_value:
                        field_value = extracted_list_value
                    elif extracted_reference:
                        field_value = extracted_reference
                    elif extracted_user:
                        field_value = extracted_user['email']
                        record_dict[f'{field_name}_FirstName'] = extracted_user['firstName']
                        record_dict[f'{field_name}_LastName'] = extracted_user['lastName']

                record_dict[field_name] = field_value

            # Append the extracted record to the records list.
            records_list.append(record_dict)
        return records_list

    def upload_dataframe_to_sharepoint(self, df, context, write_file_name):
        """
        Upload a DataFrame to SharePoint as a Parquet file.

        Args:
            df (pd.DataFrame): DataFrame containing data to be uploaded.
            context (Object): SharePoint context object.
            write_file_name (str): Name of the file to be written on SharePoint.

        Returns:
            bool: True if upload is successful, False otherwise.
        """
        try:
            logging.info("Starting DataFrame upload to SharePoint...")

            # Define the target folder in SharePoint and convert the DataFrame to Parquet format.
            logging.info(f"Defining target folder and converting DataFrame to Parquet format for: {write_file_name}")
            target_folder = context.web.get_folder_by_server_relative_url(f"DataInputs/ArcherDashboard/IncidentReport")
            parquet_content = df.to_parquet(index=False)

            # Upload the Parquet data to SharePoint.
            logging.info("Uploading Parquet data to SharePoint...")
            target_folder.upload_file(write_file_name, parquet_content).execute_query()
            logging.info(f"Successfully uploaded {write_file_name} to SharePoint.")
            return True
        except Exception as e:
            logging.error(f"Error while uploading DataFrame to SharePoint: {e}")
            return False



    def fetch_all_report_pages(self, archer_token, archer_incident_report_guid, url):
        """
        Fetch all paginated report pages from Archer and return as a DataFrame.

        Args:
            archer_token (str): Archer token.
            archer_incident_report_guid (str): GUID of the Archer incident report.
            url (str): API endpoint URL.

        Returns:
            pd.DataFrame: DataFrame containing all the report data.
        """
        archer_page_number = 1
        # Create an empty DataFrame to store all report pages.
        final_df = pd.DataFrame()

        # Load the headers for the API request.
        headers = self._load_headers('templates/headers/search_records_by_report.json')

        while True:
            # Generate the XML report for the current page number.
            incident_report_xml = self.archer_incident_report_xml(archer_token, archer_incident_report_guid, archer_page_number)
            
            # Send the request to the Archer API.
            response = requests.post(url, data=incident_report_xml, headers=headers, verify=False)
            
            # Convert the response to XML format.
            xml_response = self.response_to_xml(response, 'SearchRecordsByReportResult')
            
            # If the current page has no records, break the loop.
            if not xml_response.findall('.//Record'):
                break

            # Extract headers and records from the current XML page.
            report_headers = self.get_report_headers(xml_response)
            df = pd.DataFrame(self.get_report_records(xml_response, report_headers))

            # Append the extracted data to the final DataFrame.
            final_df = pd.concat([df, final_df])
            
            # Increment the page number for the next iteration.
            archer_page_number += 1

        return final_df