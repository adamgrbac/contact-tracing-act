import pandas as pd
from urllib.parse import unquote
import sqlite3
from datetime import datetime


def convert_date(date_str: str) -> datetime:
    try:
        date_val = datetime.strptime(date_str.strip(), '%d/%m/%Y - %A').date()
    except Exception as e:
        date_val = datetime.strptime(date_str.strip(), '%d/%m%Y - %A').date()
    finally:
        return date_val
        
        
def prep_database(con: sqlite3.Connection) -> None:
    cur = con.cursor()

    # Create history table if missing
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contact_tracing_hist (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp,
           row_start_tstp timestamp,
           row_end_tstp timestamp,
           row_status_code int
        );""")

    cur.execute("""DROP TABLE IF EXISTS temp.contact_tracing_staging""")
    # Create staging table
    cur.execute("""
        CREATE TABLE temp.contact_tracing_staging (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp
        );""")

    cur.execute("""DROP TABLE IF EXISTS temp.contact_tracing_inserts""")
    # Create history table if missing
    cur.execute("""
        CREATE TABLE temp.contact_tracing_inserts (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp,
           row_start_tstp timestamp,
           row_end_tstp timestamp,
           row_status_code int
        );""")

    cur.execute("""DROP TABLE IF EXISTS temp.contact_tracing_updates""")
    # Create history table if missing
    cur.execute("""
        CREATE TABLE temp.contact_tracing_updates (
           severity varchar(256),
           data_date timestamp,
           data_location varchar(256),
           data_suburb varchar(256),
           data_datetext varchar(256),
           data_timetext varchar(256),
           data_added  timestamp,
           row_start_tstp timestamp,
           row_end_tstp timestamp,
           row_status_code int
        );""")

    cur.close()


def htmlify(df: pd.DataFrame) -> str:
    """
    Description:
        htmlify takes in a Pandas DataFrame and returns a prettified
        html version for insertion into an email.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to transform
    Returns:
        output: str - html string
    """
    df = df.sort_values(["data_suburb"])
    output = ""
    for suburb in list(df["data_suburb"].unique()):
        output += f"<h4>{suburb}</h4>"
        output += "<ul>"
        for row in df[df["data_suburb"] == suburb].to_dict(orient="records"):
            output += f"<li>({row['severity']}) {row['data_location']}, {row['data_suburb']} on {row['data_datetext']} between {row['data_timetext']}</li>"
        output += "</ul>"
    return output


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Description:
        clean_dataframe cleans a pandas DataFrame by correcting formatting
        issues and creating some desired columns.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to clean
        table_name: str - Name of contact tracing table (used as column value)
    Returns:
        df: pd.DataFrame - cleaned pandas Dataframe
    """

    col_names = list(df.columns)
    df["severity"] = df["Contact"]
    df["data_date"] = df["Date"].apply(convert_date)
    df["data_location"] = df.apply(lambda row: str(row["Exposure Site"]).strip()+", "+str(row["Street"]).strip(), axis=1)
    df["data_suburb"] = df["Suburb"]
    df["data_datetext"] = df["Date"]
    df["data_timetext"] = df.apply(lambda row: row["Arrival Time"]+" - "+row["Departure Time"],axis=1)
    df["data_added"] = datetime.now()

    df = df.drop(col_names, axis=1)
    df = df.sort_values(["data_date","data_location","data_datetext","data_timetext","data_added"])
    df = df.groupby(["data_date","data_location","data_datetext","data_timetext"]).agg({"severity":"last","data_suburb":"last","data_added":"last"}).reset_index()
    return df


def load_staging_tables(con: sqlite3.Connection) -> None:

    cur = con.cursor()

    # Create updates
    cur.execute("""
        INSERT INTO temp.contact_tracing_updates
        SELECT
            staging.*,
            time('now') as row_start_tstp,
            time('3000-12-31 23:59:59') as row_end_tstp,
            1 as row_status_code
        FROM temp.contact_tracing_staging staging
        INNER JOIN contact_tracing_hist hist ON  staging.data_date = hist.data_date
                                            AND staging.data_location = hist.data_location
                                            AND staging.data_datetext = hist.data_datetext
                                            AND staging.data_timetext = hist.data_timetext
                                            AND hist.row_status_code = 1
        WHERE
                COALESCE(hist.severity,'') <> COALESCE(staging.severity,'')
            OR  COALESCE(hist.data_suburb,'') <> COALESCE(staging.data_suburb,'')
        """)

    # Create inserts
    cur.execute("""
        INSERT INTO temp.contact_tracing_inserts
        SELECT
            staging.*,
            time('now') as row_start_tstp,
            time('3000-12-31 23:59:59') as row_end_tstp,
            1 as row_status_code
        FROM temp.contact_tracing_staging staging
        LEFT JOIN contact_tracing_hist hist ON  staging.data_date = hist.data_date
                                            AND staging.data_location = hist.data_location
                                            AND staging.data_datetext = hist.data_datetext
                                            AND staging.data_timetext = hist.data_timetext
                                            AND hist.row_status_code = 1
        WHERE
            hist.data_location IS NULL
        """)

    cur.close()


def update_historical_records(con: sqlite3.Connection) -> None:

    cur = con.cursor()

    # Update historical records
    cur.execute("""
    UPDATE contact_tracing_hist
    SET row_status_code = 0, row_end_tstp = (SELECT row_start_tstp - 1
                                             FROM temp.contact_tracing_updates
                                             WHERE  contact_tracing_updates.data_date = contact_tracing_hist.data_date
                                                AND contact_tracing_updates.data_location = contact_tracing_hist.data_location
                                                AND contact_tracing_updates.data_datetext = contact_tracing_hist.data_datetext
                                                AND contact_tracing_updates.data_timetext = contact_tracing_hist.data_timetext
                                                AND contact_tracing_hist.row_status_code = 1)
    WHERE EXISTS (SELECT data_location, data_datetext, data_timetext
                  FROM temp.contact_tracing_updates
                  WHERE  contact_tracing_updates.data_date = contact_tracing_hist.data_date
                    AND contact_tracing_updates.data_location = contact_tracing_hist.data_location
                    AND contact_tracing_updates.data_datetext = contact_tracing_hist.data_datetext
                    AND contact_tracing_updates.data_timetext = contact_tracing_hist.data_timetext
                    AND contact_tracing_hist.row_status_code = 1)""")
    cur.close()
