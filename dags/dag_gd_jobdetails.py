# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

# Python imports
import requests
import pandas as pd
import datetime

# Custom Imports
from mongo_atlas import conn_test, get_jobsearch_ids, get_jobdetails_ids, delete_document_jobsearch, insert_many_jobdetails
import job_cloud_creds


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5)
}

# Workflow: check connection, fetch the unused jobIDs, extract the next 10 jobIDs, transform the Data, Save the data in Mongo Atlas

# Test if the connection to the MongoDB Atlas Server is working


def connection():
    conn_status = conn_test
    print(conn_status)

    if conn_status == False:
        return 'conn_failed'
    return 'get_required_ids'


def connection_failed():
    print("Unable to connect to the server.")


# get the job_ids which are not used until now
def get_required_ids():
    jobsearch_ids = get_jobsearch_ids()
    jobdetail_ids = get_jobdetails_ids()

    s = set(jobdetail_ids)
    required_ids = [x for x in jobsearch_ids if x not in s]
    next_job_ids = required_ids[:2]
    print(required_ids)

    return next_job_ids


def extract_transform_jobdetals(ti, **kwargs):
    next_job_ids = ti.xcom_pull(task_id='get_next_ids') # pulls the data from the airflow database which contains the next ids
    
    # for each job ID in the list next_job_ids we will query the jobdetails, transform the response and save the response to the total dataframe
    for id in next_job_ids:
        url = f"https://glassdoor.p.rapidapi.com/job/{id}"

        headers = {
            "X-RapidAPI-Key": f"{job_cloud_creds.rapid_api_key}",
            "X-RapidAPI-Host": "glassdoor.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers)

        # check if the jobdetails for the jobid are still existing. If not the API will return a status code 500
        # if the jobdetails aren´t available anymore, we will delete the document of this jobid from the jobseacrh collection in MongoDB
        if response.status_code == 500:
            print(f'jobdetails for id {id} not available')
            delete_dict = {"id": id}
            delete_document_jobsearch(delete_dict)
            print(
                f'document for id {id} is deleted from the jobsearch collection')
        else:
            res_json = response.json()

            # the first key:value par (company) contains the company details, we only want
            transformed_dict = {
                "job_id": id,
                "company_name": res_json['company']['name'],
                "company_id": res_json['company']['id'],
                "creation_date": res_json['creation_date'],
                "job_title": res_json['job_title'],
                "location": res_json['location'],
                "description": res_json['description'],
                "api": "Glassdoor"
            }

            df_transformed = pd.DataFrame(transformed_dict, index=[0])

            # concat the extract dataframe to the global Dataframe
            df_jobdetails_total = pd.concat(
                [df_jobdetails_total, df_transformed], axis=0, ignore_index=True)


def load_jobdetails(**kwargs):
    count_row = df_jobdetails_total.shape[0]

    jobdetails_total_dict = df_jobdetails_total.to_dict('records')
    insert_many_jobdetails(jobdetails_total_dict)
    print(f'{count_row} rows uploaded to MongoDB-Atlas cloud')


df_jobdetails_total = pd.DataFrame()

with DAG("glassdoor_jobdetails", start_date=datetime(2023, 1, 1),
         schedule_interval="@daily", tags=['Job-Analytics'], default_args=default_args, catchup=False) as dag:

    conn_check = BranchPythonOperator(
        task_id='connection_check',
        python_callable=connection,
        provide_context=True
    )

    conn_failed = PythonOperator(
        task_id='conn_failed', 
        python_callable=connection_failed
    )

    get_ids = PythonOperator(
        task_id='get_next_ids',
        python_callable=get_required_ids,
        provide_context=True
    )

    extract_transform = PythonOperator(
        task_id='extract_transform_jobdetails',
        python_callable=extract_transform_jobdetals,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_jobdetails',
        python_callable=load_jobdetails,
        provide_context=True
    )

    conn_check >> [get_ids, conn_failed] >> extract_transform >> load
