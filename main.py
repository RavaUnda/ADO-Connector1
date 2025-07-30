# main.py
"""
ADO Connector: Retrieve and publish work items to Azure DevOps board
"""

import os
from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
import pandas as pd
import json
import numpy as np

# Azure DevOps authentication function
def authenticate_azure_devops(organization_url: str, personal_access_token: str):
    credentials = BasicAuthentication('', personal_access_token)
    connection = Connection(base_url=organization_url, creds=credentials)
    return connection

def get_work_items(connection, project, areapath, iterationpath):
    wit_client = connection.clients.get_work_item_tracking_client()
    wiql_query = f"""
    SELECT [System.Id], [System.Title], [System.State], [System.AreaPath], [System.IterationPath]
    FROM WorkItems
    WHERE [System.TeamProject] = '{project}' AND [System.AreaPath] = '{areapath}' AND [System.IterationPath] = '{iterationpath}'
    """
    wiql_results = wit_client.query_by_wiql({'query': wiql_query})
    ids = [item.id for item in wiql_results.work_items]
    print(f"Found work item IDs: {ids}")
    if not ids:
        print("No work items found for the given query.")
        return []
    all_work_items = []
    batch_size = 200
    for i in range(0, len(ids), batch_size):
        batch_ids = ids[i:i+batch_size]
        try:
            work_items = wit_client.get_work_items(batch_ids)
            all_work_items.extend([wi.as_dict() for wi in work_items])
        except Exception as e:
            print(f"Error retrieving batch {i//batch_size+1}: {e}")
    return all_work_items

def write_work_items_to_excel(work_items, filename):
    if not work_items:
        print("No work items found.")
        return
    # Extract fields from each work item
    records = []
    for wi in work_items:
        fields = wi.get('fields', {})
        record = {k.split('.')[-1]: v for k, v in fields.items()}
        record['Id'] = wi.get('id')
        # Extract displayName for AssignedTo if present
        assigned_to = record.get('AssignedTo')
        if isinstance(assigned_to, dict):
            record['AssignedTo'] = assigned_to.get('displayName', '')
        records.append(record)
    df = pd.DataFrame(records)
    df.to_excel(filename, index=False)
    print(f"Work items written to {filename}")

def load_config(config_path='config.json'):
    with open(config_path, 'r') as f:
        return json.load(f)

def publish_work_items_from_excel(connection, project, excel_file):
    wit_client = connection.clients.get_work_item_tracking_client()
    df = pd.read_excel(excel_file)
    updated_rows = []
    for idx, row in df.iterrows():
        fields = {}
        # Map Excel columns to Azure DevOps fields
        def clean_value(val, is_numeric=False):
            if pd.isna(val) or (isinstance(val, float) and np.isnan(val)):
                return 0 if is_numeric else ''
            return val
        if 'Title' in row:
            fields['System.Title'] = clean_value(row['Title'])
        if 'State' in row:
            fields['System.State'] = clean_value(row['State'])
        if 'AreaPath' in row:
            fields['System.AreaPath'] = clean_value(row['AreaPath'])
        if 'IterationPath' in row:
            fields['System.IterationPath'] = clean_value(row['IterationPath'])
        if 'AssignedTo' in row:
            fields['System.AssignedTo'] = clean_value(row['AssignedTo'])
        if 'Description' in row:
            fields['System.Description'] = clean_value(row['Description'])
        if 'OriginalEstimate' in row:
            fields['Microsoft.VSTS.Scheduling.OriginalEstimate'] = clean_value(row['OriginalEstimate'], is_numeric=True)
        if 'RemainingWork' in row:
            fields['Microsoft.VSTS.Scheduling.RemainingWork'] = clean_value(row['RemainingWork'], is_numeric=True)
        if 'CompletedWork' in row:
            fields['Microsoft.VSTS.Scheduling.CompletedWork'] = clean_value(row['CompletedWork'], is_numeric=True)
        document = [{'op': 'add', 'path': f'/fields/{k}', 'value': v} for k, v in fields.items()]
        # Set parent if provided
        if 'ParentId' in row and not pd.isna(row['ParentId']):
            document.append({
                'op': 'add',
                'path': '/relations/-',
                'value': {
                    'rel': 'System.LinkTypes.Hierarchy-Reverse',
                    'url': f"https://dev.azure.com/{project}/_apis/wit/workItems/{int(row['ParentId'])}"
                }
            })
        # Only update if Id is present and not blank
        if 'Id' in row and not pd.isna(row['Id']) and str(row['Id']).strip() != '':
            try:
                updated_item = wit_client.update_work_item(
                    document=document,
                    id=int(row['Id'])
                )
                print(f"Updated task: {fields.get('System.Title', '')} | ID: {updated_item.id}")
                row['Id'] = updated_item.id
            except Exception as e:
                print(f"Error updating task: {e}")
        # Only create if Id is blank
        elif 'Id' not in row or pd.isna(row['Id']) or str(row['Id']).strip() == '':
            try:
                created_item = wit_client.create_work_item(
                    document=document,
                    project=project,
                    type='Task'
                )
                print(f"Published task: {fields.get('System.Title', '')} | ID: {created_item.id}")
                row['Id'] = created_item.id
            except Exception as e:
                print(f"Error publishing task: {e}")
        updated_rows.append(row)
    # Write back updated Ids to Excel
    updated_df = pd.DataFrame(updated_rows)
    updated_df.to_excel(excel_file, index=False)

def create_blank_publish_excel(filename):
    columns = ['Title', 'State', 'AreaPath', 'IterationPath', 'AssignedTo', 'Description', 'OriginalEstimate', 'RemainingWork', 'CompletedWork', 'ParentId']
    df = pd.DataFrame(columns=columns)
    df.to_excel(filename, index=False)
    print(f"Blank template created: {filename}")

# Example usage (replace with your values)
# Remove hardcoded ORGANIZATION_URL and PERSONAL_ACCESS_TOKEN

if __name__ == "__main__":
    config = load_config()
    connection = authenticate_azure_devops(config["organization_url"], config["personal_access_token"])
    print("Authenticated to Azure DevOps.")
    project = config["project"]
    areapath = config["areapath"]
    iterationpath = config["iterationpath"]
    work_items = get_work_items(connection, project, areapath, iterationpath)
    write_work_items_to_excel(work_items, 'work_items.xlsx')
    # Create a blank Excel template only if it doesn't exist
    publish_file = 'work_items_to_publish.xlsx'
    if not os.path.exists(publish_file):
        create_blank_publish_excel(publish_file)
    # Publish work items from Excel
    publish_work_items_from_excel(connection, project, publish_file)
