import datetime
import requests
import job_credentials
import pandas as pd
import mongosetup
import time

def week():
    act_week = int(datetime.date.today().isocalendar()[1])
    return act_week

def year():
    act_year = int(datetime.date.today().isocalendar()[0])
    return act_year


# import of the 20 biggest tech cities from Germany
df_cities = pd.read_excel(r'D:/Projekte/Job-Analytics/tech_hubs_germany_eng.xlsx', index_col=0)

city_names = df_cities['Stadt'].tolist()
city_state = df_cities['Bundesland'].tolist()
job_title = ['Data Engineer', 'Analytics Engineer', 'Data Analyst', 'Data Scientist']

# Global dataframe for the extracts of each city
df_salarys = pd.DataFrame()

# API variables
url = "https://job-salary-data.p.rapidapi.com/job-salary"
headers = {
	"X-RapidAPI-Key": f"{job_credentials.rapid_api_key}",
	"X-RapidAPI-Host": "job-salary-data.p.rapidapi.com"
}


### EXTRACT the salary data for each job title and for each city 
for job in job_title:

    state_list_index = 0

    for city in city_names:

        querystring = {"job_title":f"{job}","location":f"{city}, Germany","radius":"0"}
        response = requests.request("GET", url, headers=headers, params=querystring)
        print(response.text)
        
        res_json = response.json()

        # we only want the salary data of the response in our dataframe. 
        # Otherwise all the needed data would be stored in on dict in one column
        df_extract = pd.DataFrame(res_json['data'])

        # adding the city, state and the searched jobtitle to the response dataframe
        df_extract['city'] = f'{city}'
        df_extract['state'] = city_state[state_list_index]
        df_salarys['searched_title'] = job
        state_list_index += 1
        
        df_salarys = pd.concat([df_salarys, df_extract], axis=0, ignore_index=True)

        time.sleep(2)


### TRANSFORM the global dataframe
df_salarys.drop('publisher_link', axis=1)
df_salarys['year'] = year()
df_salarys['week'] = week()
df_salarys = df_salarys.drop_duplicates()
print(df_salarys)


### LOADING 
df_salarys.to_excel(f'D:/Projekte/Job-Analytics/data/salarys/salarys_{year()}_{week()}.xlsx', index=False)
df_salarys.to_json(f'D:/Projekte/Job-Analytics/data/salarys/salarys_{year()}_{week()}.json')


# Loading the dataframe jobdetails_total to MongoDB
salarys_dict = df_salarys.to_dict('records')
mongosetup.insert_many_salarys(salarys_dict)
print('rows uploaded to MongoDB')