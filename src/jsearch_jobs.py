import requests
import pandas as pd
import job_credentials
import datetime
import mongosetup


url = "https://jsearch.p.rapidapi.com/search"

headers = {
            "X-RapidAPI-Key": f"{job_credentials.rapid_api_key}",
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }


df_jsearch_jobs_total = pd.DataFrame()

countrys = ['Germany', 'USA']

# quering the results for the USA and Germany. More pages for the USA (5) because there are more jobs published.
# date_posted "week" that only new jobs will be selected
for country in countrys:
    page = 1
    

    if country == 'USA':

        while page <= 5:
            querystring = {"query":f"Analytics Engineer in {country}","page":f"{page}","date_posted":"week"}
            response = requests.request("GET", url, headers=headers, params=querystring)

            print(page)
            print(type(response))

            res_json = response.json()
            df_jsearch_jobs = pd.DataFrame(res_json['data'])
            print(df_jsearch_jobs)
            page += 1

            # Concat the actual results to the total dataframe
            df_jsearch_jobs_total = pd.concat(
                [df_jsearch_jobs_total, df_jsearch_jobs], axis=0, ignore_index=True)

    else:
        while page <= 3:
            querystring = {"query":f"Analytics Engineer in {country}","page":f"{page}","date_posted":"week"}
            response = requests.request("GET", url, headers=headers, params=querystring)

            print(page)
            print(type(response))

            res_json = response.json()
            df_jsearch_jobs = pd.DataFrame(res_json['data'])
            print(df_jsearch_jobs)
            page += 1

            # Concat the actual results to the total dataframe
            df_jsearch_jobs_total = pd.concat(
                [df_jsearch_jobs_total, df_jsearch_jobs], axis=0, ignore_index=True)


def week():
    act_week = int(datetime.date.today().isocalendar()[1])
    return act_week

def year():
    act_year = int(datetime.date.today().isocalendar()[0])
    return act_year


# Deleting possible duplicates in the dataframe based on the column job_id
df_jsearch_jobs_total.drop_duplicates(subset=['job_id'])
print(df_jsearch_jobs_total)


# Searching documents from MongoDB for already existing results. Deleting them from the dataframe.
existing_job_ids = mongosetup.get_jsearch_ids()
print(existing_job_ids)
print(len(existing_job_ids))
df_jsearch_jobs_total = df_jsearch_jobs_total[~df_jsearch_jobs_total.job_id.isin(existing_job_ids)]


# Saving concated dataframe with all jobdetails
df_jsearch_jobs_total.to_excel(f'D:/Projekte/Job-Analytics/data/jsearch_jobs/jsearch_jobs_{year()}_{week()}.xlsx', index=False)
df_jsearch_jobs_total.to_json(f'D:/Projekte/Job-Analytics/data/jsearch_jobs/jsearch_jobs_{year()}_{week()}.json')
print(df_jsearch_jobs_total)


# Loading the dataframe jobdetails_total to MongoDB
jsearch_total_dict = df_jsearch_jobs_total.to_dict('records')
mongosetup.insert_many_jsearch_jobs(jsearch_total_dict)
print('rows uploaded to MongoDB')