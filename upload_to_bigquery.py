from google.cloud import storage
import os
import pandas as pd
from pandas.io import gbq

import logging
from io import StringIO
from upload_to_storage.upload_storage import gcs
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.client import Client

#creating google api credential from service_account
credential=service_account.Credentials.from_service_account_file('/Users/ashutoshgoyal/Downloads/gcp-assignment-322710-592f9fdf6012.json')
class bigquery1:
    # downloading csv from google cloud storage
    def downloading_csv(self):
        #error handling using try and except
        try:
          g=gcs()
          b=g.getbucket()

          blob_customers=b.blob("Customers.csv")
          blob_orders=b.blob("Orders.csv")
          blob_customers.download_to_filename("Customers1.csv")
          blob_orders.download_to_filename("Orders1.csv")
        except Exception as e:
            logging.error("error occured",e)
#     #left join of two csvs
    def left_join_csv(self):
        df_orders=pd.read_csv("/Users/ashutoshgoyal/Downloads/gcp/Orders1.csv")


        df_customers=pd.read_csv("/Users/ashutoshgoyal/Downloads/gcp/Customers1.csv")

        resultant_df=pd.merge(df_customers,df_orders,on='CustomerID',how='left')
        resultant_df.to_csv("customer_order.csv",index=False)
        return resultant_df
 # create a bigquery_table and write_resultant_dataset
    def create_and_write_resultant_data(self,df):
         #error handling using try and except
         try:
             df.to_gbq(destination_table='upload_to_bigquery.table2',project_id='gcp-assignment-322710',if_exists='fail')

         except Exception as e:
             # use logging
             logging.error("error",e)


if __name__=="__main__":
      # making object  of bigquery1 class
      big=bigquery1()
      big.downloading_csv()
      s=big.left_join_csv()

      big.create_and_write_resultant_data(s)

