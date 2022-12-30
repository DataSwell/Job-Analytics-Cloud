import requests
import job_cloud_creds
import pandas as pd
import datetime
import mongo_atlas


# Select the job_ids from the jobsearch collection which are not in the jobdetails collection
def get_required_ids():
    jobsearch_ids = mongo_atlas.get_jobsearch_ids()
    print('Jobsearch_ids:\n', jobsearch_ids)
    jobdetail_ids = mongo_atlas.get_jobdetails_ids()
    print('Jobdetail_ids:\n', jobdetail_ids)

    s = set(jobdetail_ids)
    required_job_ids = [x for x in jobsearch_ids if x not in s]
    print('Required_ids:\n', required_job_ids)
    return required_job_ids


required_ids = get_required_ids()
next_job_ids = required_ids[:10]
print(next_job_ids)

df_jobdetails_total = pd.DataFrame()


# API request for next 10 job details
# for each job ID in the list next_job_ids we will query the jobdetails, transform the response and save the response to the total dataframe
for ids in next_job_ids:
    url = f"https://glassdoor.p.rapidapi.com/job/{ids}"

    headers = {
        "X-RapidAPI-Key": f"{job_cloud_creds.rapid_api_key}",
        "X-RapidAPI-Host": "glassdoor.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)

    # check if the jobdetails for the jobid are still existing. If not the API will return a status code 500
    # if the jobdetails aren´t available anymore, we will delete the document of this jobid from the jobseacrh collection in MongoDB
    print(response.status_code)
    print(type(response.status_code))
    
    if response.status_code == 500:
        print(f'jobdetails for id {ids} not available')
        delete_dict = {"id": ids}
        mongo_atlas.delete_document_jobsearch(delete_dict)
        print(f'document for id {ids} is deleted from the jobsearch collection')
    else:
        res_json = response.json()
        print(res_json)
        print(type(res_json))

        # if we don´t transform the response we can´t load it properly in a dataframe
        # the first key:value par (company) contains the company details, we only want
        transformed_dict = {
            "job_id": ids,
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


# Saving the dataframe with all jobdetails
today = datetime.date.today()

df_jobdetails_total.to_excel(
    f'D:/Projekte/Job-Analytics/data/gd_jobdetails/gd_jobdetails_total_{today}.xlsx', index=False)
df_jobdetails_total.to_json(
    f'D:/Projekte/Job-Analytics/data/gd_jobdetails/gd_jobdetails_total_{today}.json')


# Loading the dataframe jobdetails_total to MongoDB
jobdetails_total_dict = df_jobdetails_total.to_dict('records')
mongo_atlas.insert_many_jobdetails(jobdetails_total_dict)
print('rows uploaded to MongoDB')
