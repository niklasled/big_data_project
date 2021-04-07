# imports
import pandas as pd
import numpy as np
import smtplib, ssl
import requests
from bs4 import BeautifulSoup
from datetime import date
from pymongo import MongoClient


## definitions ##
# checks for nulls in pdf columns
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


# send mail from gmail account
def send_mail(message):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = "bigdataprojectdhbw@gmail.com"
    receiver_email = "niklas.lederer99@gmail.com"
    password = ""  # set password here

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message)


# indicates if pipe fails
succeed = True

# set error messages for mails
message1 = ""
message2 = ""
message3 = ""
message4 = ""

# get table from web source
resp = requests.get("https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Fallzahlen.html")
soup = BeautifulSoup(resp.text, 'html.parser')
table = soup.find_all('tr')
Liste = []

# split html table to pandas df
try:
    for i in range(2, 18):
        zeile = table[i]
        temp = zeile.get_text(",")
        temp = temp.split(",")
        if len(temp) > 6:
            temp[0] = temp[0] + temp[1]
            temp[0] = temp[0].replace("\n", "")
            temp.pop(1)
        Liste.append(temp)

    df = pd.DataFrame(Liste,
                      columns=['bundesland', 'anzahl', 'diff_zu_vortag', 'faelle_letzte_7_tage', '7_tage_inzidenz',
                               'todesfaelle'], )
except:
    # set message if read data fails
    message1 = "Read Data from HTML failed!\n\n"
    # set succeed to false (no write at the end)
    succeed = False

# type cast to final data type
try:
    df["anzahl"] = df["anzahl"].str.replace(".", "")
    df["diff_zu_vortag"] = df["diff_zu_vortag"].str.replace(".", "")
    df["faelle_letzte_7_tage"] = df["faelle_letzte_7_tage"].str.replace(".", "")
    df["7_tage_inzidenz"] = df["7_tage_inzidenz"].str.replace(".", "")
    df["todesfaelle"] = df["todesfaelle"].str.replace(".", "")
    df = df.astype(
        {'anzahl': 'int64', 'diff_zu_vortag': 'int64', 'faelle_letzte_7_tage': 'int64', '7_tage_inzidenz': 'float64',
         'todesfaelle': 'int64'})
except:
    # set message if type cast fails
    message2 = "Data Type Cast to int 64 failed!\n\n"
    # set succeed to false (no write at the end)
    succeed = False

# calculate row for Bundesrepublik
df_brd = df.copy()
df_brd["bundesland"] = "Bundesrepublik"
df_brd = df_brd.groupby("bundesland").agg(
    {'anzahl': 'sum', 'diff_zu_vortag': 'sum', 'faelle_letzte_7_tage': 'sum', '7_tage_inzidenz': 'mean',
     'todesfaelle': 'sum'}).reset_index()

# append new row to original df
df = pd.concat([df, df_brd], ignore_index=True)

# add column with current date
df["datum"] = str(date.today())

## check data quality ##

# create df for checks
types = df.dtypes.to_list()
columns = df.columns.to_list()
nulls = check_for_nulls(df)
# set expected data types
expected_types = ['object', 'int64', 'int64', 'int64', 'float64', 'int64', 'object']

df_check = pd.DataFrame(list(zip(columns, types, nulls, expected_types)),
                        columns=['col_name', 'data_type', 'contains_nulls', 'expected_types'])
df_check['type_error'] = np.where(df_check['data_type'] == df_check['expected_types'], 0, 1)

# check for errors
if len(df_check[df_check["contains_nulls"] == 1]) > 0:
    # set succeed to false (no write at the end)
    succeed = False
    col_list = ', '.join(df_check[df_check["contains_nulls"] == 1]["col_name"].to_list())
    # set message df contains null values (and paste column name)
    message3 = "Error in columns " + col_list + "\n   -- columns contain null values\n\n"
if len(df_check[df_check["type_error"] == 1]) > 0:
    # set succeed to false (no write at the end)
    succeed = False
    col_list = ', '.join(df_check[df_check["contains_nulls"] == 1]["col_name"].to_list())
    # set message data type of a column is not as expected (and paste column name)
    message4 = "Error in columns " + col_list + "\n   -- data type not as expected\n\n"

# set message for mail with all error messages
message = """\
Subject: PIPE FAILED: RKI COVID

Your data pipe RKI COVID IMPORT failed.\n\n""" + message1 + message2 + message3 + message4

#  write Mongo DB if nothing went wrong (succeed = True)

if succeed:
    try:
        data = df.copy()

        # Connect to MongoDB
        client = MongoClient(
            "mongodb+srv://data_pipe_01:Egal1234@cluster0.9vddk.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        # set client
        db = client['big_data_project']  # change before running!!!
        # set collection
        collection = db['rki_covid_19']  # change before running!!!
        data.reset_index(inplace=True)
        # pdf to dict
        data_dict = data.to_dict("records")
        # Insert collection
        collection.insert_many(data_dict)
        print("done")
    except:
        # if write data fails -> send mail
        message = """\
        Subject: PIPE FAILED: RKI COVID

        Your data pipe RKI COVID IMPORT failed.\n\nWriting Data to Mongo DB failed!"""
        send_mail(message)
else:
    # if succeed = False send mail
    send_mail(message)
