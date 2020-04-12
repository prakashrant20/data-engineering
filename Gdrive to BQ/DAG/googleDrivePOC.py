from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from apiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

import logging
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import logging
from airflow.utils.dates import days_ago


args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

def downloadFile(drive_service, file_id,filepath):
    """Downloads the file from Google drive to GCS"""

    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))
    with io.open(filepath,'wb') as f:
        fh.seek(0)
        f.write(fh.read())
    return True


def SearchDrive2(**kwargs):
    """Checks in a particular folder for a matching file pattern and downloads it to local"""

    #Google Driver Service
    drive_service = build('drive', 'v2')

    #Folder to Search for input file
    folder_id = "1Ma5j64OUIwsBstTiaZ-K68PqYncLqdbt"
    search_file_pattern = "test_employee"
    # Local staging GCS location
    gcsLocation = "/home/airflow/gcs/data/googleDrivePOC/"


    #List the files in the folder
    page_token = None
    while True:
        try:
            param = {}
            if page_token:
                param['pageToken'] = page_token
            children = drive_service.children().list(
                folderId=folder_id, **param).execute()

            for child in children.get('items', []):
                logging.info('str(child) is {}'.format(str(child)))
                logging.info('File Id: %s' % child['id'])
                #Get the metadata of the current file
                cur_file = drive_service.files().get(fileId=child['id']).execute()
                logging.info('File name: {}'.format(cur_file['title']))

                #Check if the current file matches the search pattern
                if search_file_pattern in cur_file['title']:
                    logging.info(
                        'File matching the search condition - name =  {} : id =  ({})'.format(cur_file['title'], cur_file['id']))
                    # Download the files matching the search query
                    outputGCSFile = gcsLocation + cur_file['title']
                    if downloadFile(drive_service, cur_file['id'], outputGCSFile):
                        logging.info("Successfully downloaded the file - {} to GCS".format(outputGCSFile))
                    else:
                        logging.info("Failed to download the file- {}".format(outputGCSFile))
                else:
                    #Not what we are looking for. Ignore the current file
                    logging.info("File not matching the search pattern - File ignored {}".format(cur_file['title']))
            page_token = children.get('nextPageToken')
            if not page_token:
                break
        except Exception as e:
            print('An error occurred: {}'.format(str(e)))
            break
    return True

def SearchDrive3(**kwargs):
    """Use Drive V3 - Checks all over the drive and downloads it to local"""

    #Google Driver Service
    drive_service = build('drive', 'v3')

    # Search parameter
    query = "name contains 'test_employee' and createdTime > '2020-04-01T12:00:00'"
    # Local staging GCS location
    gcsLocation = "/home/airflow/gcs/data/googleDrivePOC/"

    results = drive_service.files().list(
         fields="nextPageToken, files(id, name, kind, mimeType)", q=query).execute()
    items = results.get('files', [])
    if not items:
        logging.info('No files found.')
    else:
        logging.info('Files:')
        for item in items:
            # logging.info(item)
            logging.info('File matching the search condition - name =  {} : id =  ({})'.format(item['name'], item['id']))
            #Download the files matching the search query
            outputGCSFile = gcsLocation + item['name']
            if downloadFile(drive_service, item['id'], outputGCSFile):
                logging.info("Successfully downloaded the file - {} to GCS".format(outputGCSFile))
            else:
                logging.info("Failed to download the file- {}".format(outputGCSFile))
    return True

with DAG('googleDrivePOC1', schedule_interval="0 23 * * 2", catchup=False, default_args=args,
         ) as dag:

    SearchDrive = PythonOperator(
        task_id='SearchDrive',
        python_callable=SearchDrive2,
        provide_context=True,
    )

    #Location to pick the file to be loaded to BQ
    gcsBucket = "us-central1-racentral-compo-31784baf-bucket"
    gcsSubDirectory = "data/googleDrivePOC/"

    loadToBigQuery = GoogleCloudStorageToBigQueryOperator(
        task_id='loadToBigQuery',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        bucket=gcsBucket,
        skip_leading_rows=0,
        source_objects=[gcsSubDirectory + "test_employee*.csv"],
        source_format='CSV',
        schema_object=gcsSubDirectory + "test_employee_schema.json",
        schema_fields=None,
        create_disposition='CREATE_IF_NEEDED',
        destination_project_dataset_table="gcp-wow-food-ra-central-dev.operation_work_area.test_employee",
        quote_character='"',
        allow_quoted_newlines=True,
        write_disposition="WRITE_APPEND",
        dag=dag,
    )

SearchDrive >> loadToBigQuery