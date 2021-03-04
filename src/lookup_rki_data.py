import pandas as pd
from pymongo import MongoClient

# read from Mongo DB
client =  MongoClient("mongodb+srv://data_pipe_01:Egal1234@cluster0.9vddk.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database("big_data_project")
records = db.rki_covid_19

liste = list(records.find())
pdf = pd.DataFrame(liste)

pdf = pdf.drop(["_id", "index"], axis = 1)
print(pdf)