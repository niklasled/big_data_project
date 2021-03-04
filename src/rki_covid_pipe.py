import pandas as pd
import numpy as np
import smtplib, ssl
import requests
from bs4 import BeautifulSoup
from datetime import date
from pymongo import MongoClient

## definitions ##
def check_for_nulls(df):
    nulls = []
    for x in columns:
        has_nulls = False
        for y in df[x].isnull():
            if y == True:
                has_nulls = True
        if has_nulls:
            nulls.append(1)
        else:
            nulls.append(0)
    return nulls

def send_mail(message):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "bigdataprojectdhbw@gmail.com"
    receiver_email = "niklas.lederer99@gmail.com"
    password = "Egal1234"

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)

succeed = True

message1 = ""
message2 = ""
message3 = ""
message4 = ""

resp = requests.get("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html")
soup = BeautifulSoup(resp.text, 'html.parser')
table = soup.find_all('tr')
Liste = []

try:
    for i in range(2,18):
        zeile = table[i]
        temp = zeile.get_text(",")
        temp = temp.split(",")
        if len(temp) > 6:
            temp[0] = temp[0] + temp[1]
            temp[0] = temp[0].replace("\n", "")
            temp.pop(1)
        Liste.append(temp)

    df = pd.DataFrame(Liste,columns = ['bundesland','anzahl','diff_zu_vortag','faelle_letzte_7_tage', '7_tage_inzidenz', 'todesfaelle'], )
except:
    message1 = "Read Data from HTML failed!\n\n"
    succeed = False

# TYPE CASTEN
try:
    df["anzahl"] = df["anzahl"].str.replace(".", "")
    df["diff_zu_vortag"] = df["diff_zu_vortag"].str.replace(".", "")
    df["faelle_letzte_7_tage"] = df["faelle_letzte_7_tage"].str.replace(".", "")
    df["7_tage_inzidenz"] = df["7_tage_inzidenz"].str.replace(".", "")
    df["todesfaelle"] = df["todesfaelle"].str.replace(".", "")
    df = df.astype({'anzahl': 'int64', 'diff_zu_vortag': 'int64', 'faelle_letzte_7_tage': 'int64', '7_tage_inzidenz': 'int64', 'todesfaelle': 'int64'})
except:
    message2 = "Data Type Cast to int 64 failed!\n\n"
    succeed = False

# ZEILE FÜR BRD BERECHNEN
df_brd = df.copy()
df_brd["bundesland"] = "Bundesrepublik"
df_brd = df_brd.groupby("bundesland").agg({'anzahl':'sum', 'diff_zu_vortag':'sum', 'faelle_letzte_7_tage':'sum', '7_tage_inzidenz':'mean', 'todesfaelle':'sum'}).reset_index()

df =  pd.concat([df, df_brd], ignore_index=True)

df["datum"] = str(date.today())

## check data quality ##

# create df for checks

types = df.dtypes.to_list()
columns = df.columns.to_list()
nulls = check_for_nulls(df)
expected_types = ['object', 'int64', 'int64', 'int64', 'float64', 'int64', 'object']


df_check = pd.DataFrame(list(zip(columns, types, nulls, expected_types)),
                        columns =['col_name', 'data_type', 'contains_nulls', 'expected_types'])
df_check['type_error'] = np.where(df_check['data_type'] == df_check['expected_types'], 0, 1)


# check for errors

if len(df_check[df_check["contains_nulls"] == 1]) > 0:
    succeed = False
    col_list = ', '.join(df_check[df_check["contains_nulls"] == 1]["col_name"].to_list())
    message3 = "Error in columns " + col_list +"\n   -- columns contain null values\n\n"
if len(df_check[df_check["type_error"] == 1]) > 0:
    succeed = False
    col_list = ', '.join(df_check[df_check["contains_nulls"] == 1]["col_name"].to_list())
    message4 = "Error in columns " + col_list +"\n   -- data type not as expected\n\n"

message = """\
Subject: PIPE FAILED: RKI COVID

Your data pipe RKI COVID IMPORT failed.\n\n""" + message1 + message2 + message3 + message4

# Mongo DB write

if succeed:
    try:
        data = df.copy()

        # Connect to MongoDB
        client =  MongoClient("mongodb+srv://data_pipe_01:Egal1234@cluster0.9vddk.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        db = client['big_data_project']
        collection = db['rki_covid_19']
        data.reset_index(inplace=True)
        data_dict = data.to_dict("records")
        # Insert collection
        collection.insert_many(data_dict)
        print("done")
    except:
        message = """\
        Subject: PIPE FAILED: RKI COVID

        Your data pipe RKI COVID IMPORT failed.\n\nWriting Data to Mongo DB failed!"""
        send_mail(message)
else:
    send_mail(message)