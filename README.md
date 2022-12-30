# Job-Analytics

## Used Job Apis
### Glassdoor job postings API (max. 20 requests/day):
https://rapidapi.com/mantiks-mantiks-default/api/glassdoor <br/>
This API has different endpoints. For our use cases we use the two endpoints: <br/>
GET Jobs Search -> returns per request a list of 30 jobs for a keywword (data engineer), locationId (96 = Germany). 
In this list is the job_id, which is required for the next endpoint. <br/>
GET Job details -> with the job_ids from the first endpoint, we can get the details/content of the job posting. You can query one job posting per request.
<br/>
Because it is limited to 20 API calls per day, we want to get every two weeks 150 job_ids, which are saved to the database. Every Day we want to query the job details for 10 jobs, which haven´t been queried before. 
So we have to search for job_ids in the jobsearch-collection which are not already in the jobdetail-colection. 
If we already have all the jobdeails of the job_ids in the jobsearch collection we want to query more job_ids with the jobsearch endpoint.

### Job Salary Data (max. 500 requests/month):
https://rapidapi.com/letscrape-6bRBa3QguO5/api/job-salary-data <br/>
The API returns the Salary for a specific Job title in a specific location, based on the data of a few publisher (Payscale, Glassdoor, Salary.com, ZipRecruiter).
The salarys for Data Engineers, Analytic Engineers, Data Analysts and Data Scientist of the 20 biggest tech cities in Germany will be queried every week.

### JSearch API (max. 50 requests/month):
Using the API it to get job listenings for the newer job Analytics Engineer. Because we have less requests than with glassdoor, but the API access multiple job publisher we use this one for the job with less job postings. 
Every Request get 10 job postings. Every week we query 50 jobs for Analytics Engineer in the USA and 30 postings from Germany.


## Used Tools
- Python
- MongoDB
- Processing ... 
- Visualization ...
- Airflow ...

 
## Workflow / update intervall
Glassdoor-Jobsearch = update every Sunday (5 requests for 150 job ids) <br/>
Glassdoor-Jobdetails = 20 jobdetails everyday, except Sunday so we don´t request more than the free amount <br/>
JSearch = weekly, 50 job postings for Analytic Engineers in the USA and 30 for Germany <br/>
Job Salarys = weekly (4 different Jobs in 20 biggest tech cities in germany)


