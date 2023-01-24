import pandas as pd
import datetime
import mongo_atlas

skills = ['Python', 'SQL', 'NoSQL', 'Airflow', 'Git', 'Docker', 'Kafka', 'Spark', 'AWS', 'Azure', 'GCP', 'Big Query', 
'ETL', 'ELT', 'Modeling', 'Warehousing', 'Snowflake', 'dbt', 'DBT', 'Qlik', 'PowerBI', 'Tableau', 'Kimball', 'Inmon', 'Data Vault',
'Datenmodellierung', 'Data Modeling', 'Dimensional', 'Visualization', 'Analysen', 'Version Control', 'CI/CD', 'Testing']

skills_test = ['Python', 'SQL', 'NoSQL']

# db.getCollection("movies").find({'genre':/Comedy/})

# find he descriptions with the keyords
for skill in skills_test:
    mongo_atlas.get_jobdetails_description('description', skill)


# update the field 'keywords' wich is an array with the founded keyword


# the new key of the array will be the last one + 1
# dict.keys()[-1]