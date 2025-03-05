import pandas as pd;
import glob;
from datetime import datetime;
import xml.etree.ElementTree as ET;


log_file = "log_file.txt";
main_file = "transformed_data.csv";

#  extracting from csv file 
def extract_from_csv(file_process):
    data_frame = pd.read_csv(file_process);
    return data_frame;

def extract_from_json(file_process):
    data_frame = pd.read_json(file_process, lines=True);
    return data_frame;

def extract_from_xml(file_process):
    data_frame = pd.DataFrame(columns= ["car_model", "year_of_manufacture", "price", "fuel"]);
    tree = ET.parse(file_process);
    root = tree.getroot();
    for row in root:
        car_model = row.find("car_model").text;
        year_of_manufacture = row.find("year_of_manufacture").text;
        price = row.find("price").text;
        fuel = row.find("fuel").text;
        data_frame = pd.concat([data_frame, pd.DataFrame([{
            "car_model": car_model,
            "year_of_manufacture": year_of_manufacture,
            "price": price,
            "fuel": fuel,
        }])], ignore_index=True)
    return data_frame;

def extract_data():
    extracted_data = pd.DataFrame(columns = ["car_model", "year_of_manufacture", "price", "fuel"]);
    
    for csv_file in glob.glob("*.csv"):
        if csv_file != main_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csv_file))], ignore_index=True);
    
    for json_file in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(json_file))], ignore_index=True);
    
    for xml_file in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xml_file))], ignore_index=True);
    return extracted_data;



def transform(data):
    data["price"]  = data["price"].astype(float)*2;
    data["price"]  = data["price"].round(2);
    return data;

def data_load(main_file, transformed_data):
    transformed_data.to_csv(main_file)
    
def log_prosses(message):
    time_stamp_format = '%Y-%h-%d-%H:%M:%S';
    now = datetime.now();
    time_stamp = now.strftime(time_stamp_format);
    with open(log_file , "a") as file:
        file.write(time_stamp +"," + message + '\n' );

log_prosses("ETL Job Started");

log_prosses("Extract phase started");
extracted_data = extract_data();
log_prosses("Extract phase ended");

log_prosses("Data Transform started")
transformed_data = transform(extract_data());
log_prosses("Data Transform ended");

log_prosses("Data Loaded started");
data_load(main_file, transformed_data);

log_prosses("Load phase Ended") 
  
# Log the completion of the ETL process 
log_prosses("ETL Job Ended") 

