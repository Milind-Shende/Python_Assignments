"""Mysql :

    1. Create a  table attribute dataset and dress dataset
    2. Do a bulk load for these two table for respective dataset
    3. read these dataset in pandas as a dataframe
    4. Convert attribute dataset in json format
    5. Store this dataset into mongodb
    6. in sql task try to perform left join operation with attribute dataset and dress dataset on column Dress_ID
    7. Write a sql query to find out how many unique dress that we have based on dress id
    8. Try to find out how many dress is having recommendation 0
    9. Try to find out total dress sell for individual dress id
    10. Try to find out a third highest most selling dress id """
"""Requirements
   1.pip install Mysql
   2.pip install pandas
   3.pip install pymysql
   4.pip install sqlalchemy"""
import logging
logging.basicConfig(filename="Mysql.log",level=logging.DEBUG,format="%(levelname)s %(asctime)s %(name)s %(message)s")
try:
    import mysql.connector as conn
    mydb=conn.connect(host="localhost", user="root", password ="mili#2487", database="Cotton_Garments")
    import pandas as pd
    from sqlalchemy import create_engine
    engine=create_engine("mysql+pymysql://{user}:{password}@localhost/{database_name}" #Pymysql and mysql Is connectors uswd to connect database
                         .format(user="root",
                                 password="mili#2487",
                                 database_name="cotton_garments"))

    logging.info(mydb.is_connected())
    logging.info("All connection are working fine")

    attri = pd.read_excel("attribute_dataSet.xlsx",index_col=None,sheet_name='Sheet1')
    logging.info(attri)
    type(attri)
    
        
    attri.columns=(x.lower() for x in attri.columns)
    logging.info(attri.columns)
    
    # attri = attri.set_index('Dress_ID')     #Set The Index from which column it should start
    attri = attri.to_sql("attribute_dataSet", con=engine, if_exists='append')
    dress = pd.read_excel("dress_sales.xlsx",index_col=None)     #Read The File Pandas datframe
    logging.info(dress)
    type(dress)
    dress = dress.set_index('Dress_ID')
    dress.columns=pd.to_datetime(dress.columns) #format change to datetime
    dress = dress.dropna().astype('int64') #change datetime into integer
    logging.info(dress)
    dress = dress.to_csv("dress_sales1.csv")
    dress = pd.read_csv("dress_sales1.csv",index_col=None)
    
    dress.columns=(x.lower().replace('/','-') for x in dress.columns)
    logging.info(dress.columns)
    dress = dress.to_sql("Dress_sales", con=engine, if_exists='append')
except Exception as e:
    logging.error(str(e))





