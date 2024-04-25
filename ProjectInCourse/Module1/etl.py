import pandas as pd
import xml.etree.ElementTree as ET
import glob
from datetime import datetime

log_file = "log_file.txt"
stored_file = "transformed_data.csv"

def extract_csv_file(file_name):
    df = pd.read_csv(file_name)
    return df

def extract_json_file(file_name):
    df = pd.read_json(file_name, lines=True)
    return df

def extract_xml_file(file_name):
    df = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_name)
    root = tree.getroot()
    for row in root:
        name = row.find("name").text
        height = float(row.find("height").text)
        weight = float(row.find("weight").text)
        df = pd.concat([df, pd.DataFrame([{"name": name, "height": height, "weight": weight}])], ignore_index=True)
        return df
    
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])
    
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_csv_file(csvfile))], ignore_index=True)
        
    for jsonfile in glob.glob("*json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_json_file(jsonfile))], ignore_index=True)
    
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_xml_file(xmlfile))], ignore_index=True)
        
    return extracted_data

def transform(extracted_data):
    return extracted_data

def load(stored_file, transformed_data):
        transformed_data.to_csv(stored_file)
        
def log(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + "," + message + "\n")
        

# Log the initialization of the ETL process 
log("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log("Load phase Started") 
load(stored_file,transformed_data) 
 
# Log the completion of the Loading process 
log("Load phase Ended") 
 
# Log the completion of the ETL process 
log("ETL Job Ended") 