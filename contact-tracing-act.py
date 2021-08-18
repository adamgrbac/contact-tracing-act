import requests
import pandas as pd
import sqlite3
import yagmail
import utils
import yaml
from bs4 import BeautifulSoup
import re

# Load email config
with open("email_config.yml", "r") as f:
    email_config = yaml.safe_load(f)

# Setup Email details
yag = yagmail.SMTP(email_config["sender"], oauth2_file="oauth2_file.json")

# Open DB Connection
con = sqlite3.connect("contact_tracing_act.db")

# Prep database tables
utils.prep_database(con)

# GET NSW Data
res = requests.get("https://www.covid19.act.gov.au/act-status-and-response/act-covid-19-exposure-locations")

# Parse page with bs4
page = BeautifulSoup(res.text, 'html.parser')

# Get tables elements from parsed paged
csv_file = page.find_all(text=re.compile('(https.*[.]csv)'))
csv_url = re.search('(https.*[.]csv)',csv_file[0]).group(0)

# Convert list of dicts to DataFrame
df = pd.read_csv(csv_url)
# Merge dfs into one df and clean
df = utils.clean_dataframe(df)

# Load latest snapshot into tmp table
df.to_sql(name="contact_tracing_staging", con=con, schema="temp", if_exists="append", index=False)

# Break the staging table into INSERTs & UPDATEs and load into DataFrames
utils.load_staging_tables(con)
updated_records = pd.read_sql("select * from temp.contact_tracing_updates", con=con)
new_records = pd.read_sql("select * from temp.contact_tracing_inserts", con=con)

# If there are any new / updated rows, process and email to dist list
if len(new_records) > 0 or len(updated_records) > 0:

    # Email body
    contents = []

    # Create upto two sections depending on presences of new/updated records
    if len(new_records) > 0:
        contents.append("New Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(new_records))
    if len(updated_records) > 0:
        contents.append("Updated Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(updated_records))

    # Send email to dist list
    yag.send(bcc=email_config["dist_list"], subject="New ACT Contact Tracing Locations!", contents=contents)

    # Update Existing Records & Insert new records into database to mark them as processed
    utils.update_historical_records(con)
    new_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
    updated_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
else:
    # For logging purposes
    print("No updates!")

# Close DB connection
con.close()
