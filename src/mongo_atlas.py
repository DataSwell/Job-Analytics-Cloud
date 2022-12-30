import pymongo
import job_credentials


conn_str = f"mongodb+srv://DataSwell:{job_credentials.atlas_pw}@dataswellmongo.h60y77k.mongodb.net/test"

# set a 5-second connection timeout
myclient = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)

try:
    print(myclient.server_info())
except Exception:
    print("Unable to connect to the server.")


