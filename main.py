import os
from datetime import datetime, time
import requests

import pymongo
from google.cloud import storage
from settings import Settings
import certifi
import pytz

utc=pytz.UTC
settings = Settings()


# STORAGE
storage_client = storage.Client()
bucket = storage_client.bucket(settings.FITS_BUCKET)
client_mongo = pymongo.MongoClient(settings.MONGO_CONNECTION, tlsCAFile=certifi.where())
TASK_INDEX = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 1))
TASK_COUNT = int(os.environ.get("CLOUD_RUN_TASK_COUNT", 1))


def send_session(fit_file, user_info, provider_session_id):
    params_dic = {
        "user_id": user_info["_id"],
        "firestore_user_id": user_info["firestore_user_id"],
        "device_user_id": user_info["deviceUserId"],
        "device": user_info["syncedDevice"].upper(),
        "session_id": provider_session_id
    }
    try:
        res = requests.post(url=settings.NORMALIZATION_URL, data=fit_file, params=params_dic, headers={'Content-Type': 'application/octet-stream'})
    except Exception:
        return None
    
    print(res.status_code)



def get_fit(fit_name):
    file = bucket.blob(fit_name)
    try:
        return file.download_as_bytes()
    except Exception:
        return None


def check_session_exist(provider_session_id: str):
    if client_mongo.Kanara.Sessions.count_documents({"session_id": provider_session_id}) != 1:
        return False
    else: 
        return True

def check_session_exist_unverified(provider_session_id: str):
    if client_mongo.Kanara.UnverifiedSessions.count_documents({"session_id": provider_session_id}) != 1:
        return False
    else: 
        return True

def get_user_info(firestore_user_id):
    return client_mongo.Kanara.Users.find_one({"firestore_user_id": firestore_user_id})

if __name__ == "__main__":
    print(f"Task number {TASK_INDEX} started.")
    print(f"Number of tasks: {TASK_COUNT}.")
    if settings.USER_ID != "":
        sessions = storage_client.list_blobs(settings.FITS_BUCKET, prefix=settings.USER_ID)
    else:
        sessions = storage_client.list_blobs(settings.FITS_BUCKET)
    num = 0
    num2 = 0
    for session in sessions:
        print(session.name)
        if session.time_created.year >= 2022 and session.time_created.month >= 7:
            firestore_user_id = session.name.split("_")[0]
            provider_session_id = session.name.split("_")[1].split(".")[0]
            if check_session_exist(provider_session_id=provider_session_id) is True:
                continue
            if check_session_exist_unverified(provider_session_id=provider_session_id) is True:
                continue
            user_info = get_user_info(firestore_user_id)
            if user_info is None:
                continue
            fitfile = get_fit(session.name)
            send_session(fit_file=fitfile, user_info=user_info, provider_session_id=provider_session_id)


#  gcloud config set project kanarafluttertest  
#  gcloud builds submit --pack image=gcr.io/kanarafluttertest/reporcess-sessions-test

# tras esto hay que editar el job y coger la latest del contenedor de container registry
