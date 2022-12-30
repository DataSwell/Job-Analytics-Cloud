import pymongo
import pymongo.errors

# Connect to mongodb
myclient = pymongo.MongoClient(
    "mongodb://localhost:27017/", username='root', password='example')

mydb = myclient["job_analytics"]
col_jobsearch = mydb["glassdoor_job_ids"]
col_jobdetails = mydb["glassdoor_jobdetails"]
col_jsearch_jobs = mydb["jsearch_jobs"]
col_salarys = mydb["salarys"]


# Database functions
def insert_many_jobsearch(input):
    try:
        x = col_jobsearch.insert_many(input)
        print(f'{x} inserted')
    except pymongo.errors as e:
        print(e)


def insert_many_jobdetails(input):
    try:
        x = col_jobdetails.insert_many(input)
        print(x)
    except pymongo.errors as e:
        print(e)


def insert_many_jsearch_jobs(input):
    try:
        x = col_jsearch_jobs.insert_many(input)
        print(x)
    except pymongo.errors as e:
        print(e)

def insert_many_salarys(input):
    try:
        x = col_salarys.insert_many(input)
        print(x)
    except pymongo.errors as e:
        print(e)


def insert_single_jobsearch(input):
    x = col_jobsearch.insert_one(input)
    print(x)


def insert_single_jobdetails(input):
    x = col_jobdetails.insert_one(input)
    print(x)


def get_docs_jobsearch(query):
    docs = col_jobsearch.find(query)
    return docs


def get_docs_jobdetails(query=None):
    docs = col_jobdetails.find(query)
    return docs


def get_distinct_values_of_key(col, key):
    values = col.distinct(key)
    return values


def get_jobsearch_ids():
    ids = col_jobsearch.distinct('id')
    return ids


def get_jobdetails_ids():
    ids = col_jobdetails.distinct('job_id')
    return ids

def get_jsearch_ids():
    ids = col_jsearch_jobs.distinct('job_id')
    return ids

def delete_document_jobdetails(key_value_dict):
    try:
        col_jobdetails.delete_one(key_value_dict)
    except pymongo.errors as e:
        print(e)


def delete_document_jobsearch(key_value_dict):
    try:
        col_jobsearch.delete_one(key_value_dict)
    except pymongo.errors as e:
        print(e)


#insert_many(col_jobsearch, data_dict)

# ids = read_distinct_values_of_key(col_jobsearch, 'id')

# for x in col_jobdetails.find():
#     print(x)
