# +
import logging
logging.basicConfig(filename="Mongo.log",level=logging.DEBUG,format="%(levelname)s %(asctime)s %(name)s %(message)s")
try:
    """Requirements
    1.pip install pymongo
    2.pip install pandas
    3.pip install json
    """
    import json
    import pymongo
    client = pymongo.MongoClient("mongodb+srv://Milind2487:mili#2487@milind2487.olvhy.mongodb.net/?retryWrites=true&w=majority")
    db = client.test
    import pandas as pd

    """Attribute Dataset Xlsx Change to Json File and upload to Mongodb"""
    
    attri_data = pd.read_excel("attribute_dataset.xlsx", sheet_name='Sheet1')
    
    logging.info("Reading Data From Excel into panda Library")
    
    file_json = attri_data.to_json(orient='records')
    
    logging.info("Converting Excel file Into Json Format")
    
    file_json_dict = json.loads(file_json)
    
    logging.info("Converting Excel String To Dict Format Or Takes File Object And Return Into Json Object")
    
    with open("attribute_Dataset3.json", 'w') as file_json:
        
        json.dump(file_json_dict, file_json)
        
        logging.info("Send Data From Python To Json or It Convert dict  Into String")
    
    database = client["Attribute"]
    
    collection = database['converted_file']
    
    with open('attribute_Dataset3.json') as f:
        
        file_data = json.load(f)
        
        collection.insert_many(file_data)

except Exception as e:
    logging.error(str(e))
