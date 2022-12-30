import requests
import job_cloud_creds
import pandas as pd
import datetime
import mongo_atlas


def week():
    act_week = int(datetime.date.today().isocalendar()[1])
    return act_week


def year():
    act_year = int(datetime.date.today().isocalendar()[0])
    return act_year


def get_amount_open_jobids():
    jobsearch_ids = mongo_atlas.get_jobsearch_ids()
    jobdetail_ids = mongo_atlas.get_jobdetails_ids()
    s = set(jobdetail_ids)
    open_job_ids = [x for x in jobsearch_ids if x not in s]
    return len(open_job_ids)


def check_mongo_and_extract_ids(extract_ids_list, mongo_ids_list):
    s = set(mongo_ids_list)
    missing_ids = [x for x in extract_ids_list if x not in s]
    return missing_ids


# Check if there are 30 or less unused job_ids in the jobsearch collection
# if this is true we want to extract 5 new pages of job_ids
unused_job_ids = get_amount_open_jobids()
print(unused_job_ids)

if unused_job_ids <= 30:

    df_jobsearch_total = pd.DataFrame()
    next_page_cursor = ''
    next_page_id = '0'
    pages = 0

    url = "https://glassdoor.p.rapidapi.com/jobs/search"

    headers = {
        "X-RapidAPI-Key": f"{job_cloud_creds.rapid_api_key}",
        "X-RapidAPI-Host": "glassdoor.p.rapidapi.com"
    }
      
     # While loop to extract 5 pages (150 job_ids)   
    while pages < 5:

        # querystring must be in the while loop, because otherise the next_page_cursor and next_page_id will be updated, but not the querystrng variable
        # if the querystring is outside the requests will be done with the old cursor and id which are loaded by starting the script
        querystring = {
            "keyword": "Data Engineer",
            "location_id": "96", "location_type": "N",
            "page_id": f"{next_page_id}",
            "page_cursor": f"{next_page_cursor}"
        }

        print(querystring)
        response = requests.request(
            "GET", url, headers=headers, params=querystring)

        print(type(response))
        res_json = response.json()

        df_extract = pd.DataFrame(res_json['hits'])
        print(df_extract)
        print(len(df_extract.index))
        print(df_extract['id'])

        # check which ids already exist in MongoDB
        mongodb_ids = mongo_atlas.get_jobsearch_ids()
        print(len(mongodb_ids))
        print(type(mongodb_ids))

        # drop rows with ids that already exist in MongoDB
        # using the isin() function combined with the NOT operator ~
        df_extract = df_extract[~(df_extract.id.isin(mongodb_ids))]
        print(df_extract)
        print(len(df_extract.index))

        # concat the extract of this page to the total results dataframe
        df_jobsearch_total = pd.concat(
            [df_jobsearch_total, df_extract], axis=0, ignore_index=True)

        # saving the next page cursor and id for the for loop
        next_page_cursor = res_json['next_page']['next_page_cursor']
        print(next_page_cursor)
        next_page_id = res_json['next_page']['next_page_id']
        print(next_page_id)
        pages += 1
        print(pages)


    # Saving locally
    df_jobsearch_total.to_excel(
        f'D:/Projekte/Job-Analytics/data/gd_jobsearch/glassdoor_jobsearch_{year()}_{week()}.xlsx', index=False)
    df_jobsearch_total.to_json(
        f'D:/Projekte/Job-Analytics/data/gd_jobsearch/glassdoor_jobsearch_{year()}_{week()}.json')

    # Loading the jobsearch_total dataframe to MongoDB
    jobsearch_total_dict = df_jobsearch_total.to_dict('records')
    mongo_atlas.insert_many_jobsearch(jobsearch_total_dict)

else:
    print(f'{unused_job_ids} unused job ids left in database')