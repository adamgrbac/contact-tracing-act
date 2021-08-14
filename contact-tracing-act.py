import requests
import pandas as pd
import sqlite3
import yagmail
import utils
import yaml
from bs4 import BeautifulSoup

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
tables = page.find_all("table")

# Create empty list of dfs to merge later
dfs = []

# Table names
table_names = ["Close","Casual","Low"]

# Extract data from each table
for table_name, table in zip(table_names,tables):
    # Get Column Names
    headers = []
    for header in table.thead.find_all("th"):
        headers.append(header.get_text().lower().strip())
    
    # Convert <tr> attributes to list of dicts
    data = []
    for row in table.tbody.find_all("tr"):
        row_data = {"Severity": table_name}
        for col, cell in zip(headers,row.find_all("td")):
            row_data[col] = cell.get_text()
        data.append(row_data)
    
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    # Append df to list, to be merged later
    dfs.append(df)

# Merge dfs into one df and clean
df = pd.concat(dfs)
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
