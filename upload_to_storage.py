import os
import pandas as pd
import logging

#change directory using os library
os.chdir("/Users/ashutoshgoyal/downloads/gcp")
#Client for interacting with the Google Cloud Storage API
storage_client=storage.Client.from_service_account_json("gcp-assignment-322710-5cfffd0e0e6d.json")



class gcs:
    #get_bucket_from_google_cloud_storage
    def getbucket(self):




      bucket_name='bucket_problem'
      bucket=storage_client.get_bucket(bucket_name)
      return bucket

    #uploading both csvs into google cloud storage
    def uploading_csv_gcs(self,bucket):
     #error handling using try and except
     try:
      filename1="Customers.csv"
      blob1=bucket.blob(filename1)
      blob1.upload_from_filename(filename1)
      filename2="Orders.csv"

      blob2=bucket.blob(filename2)
      blob2.upload_from_filename(filename2)
     except Exception as e:
         #use logging
         logging.error("error",e)

if __name__=="__main__":
    #creating object of class gcs
    g=gcs()
    b=g.getbucket()
    g.uploading_csv_gcs(b)


