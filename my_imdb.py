import gzip
import os
import re
import sys
import urllib.request
from getpass import getpass

import pandas as pd
from mysql.connector import Error, connect, errorcode


# List of datasets download links
URLS = [
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    "https://datasets.imdbws.com/title.ratings.tsv.gz"
    ]

# MySQL Database Name
DB_NAME = "my_imdb"

# Dictionary of creating table queries
TABLES = {}
TABLES["title_basics"] = """
    CREATE TABLE title_basics (
        tconst varchar(20) NOT NULL,
        titleType varchar(20),
        primaryTitle varchar(1000),
        originalTItle varchar(1000),
        isAdult bool,
        startYear smallint,
        endYear smallint,
        runtimeMinutes int,
        genres varchar(255),
        PRIMARY KEY (tconst)
    )
    """

TABLES["title_ratings"] = """
    CREATE TABLE title_ratings (
        tconst varchar(20) NOT NULL,
        averageRating decimal(3, 1),
        numVotes int,
        FOREIGN KEY (tconst)
            REFERENCES title_basics(tconst)
    )
    """

# Dicionary of inseting queries
INSERTS = {}
INSERTS["title_basics"] = """
    INSERT INTO title_basics
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

INSERTS["title_ratings"] = """
    INSERT INTO title_ratings
    VALUES (%s, %s, %s)
    """


def get_filenames(url):
    # Get a list of filenames and filepath from url or the donwload link

    filenames = {}
    cwd = os.getcwd()
    filenames["zip_filename"] = url.rsplit("/", 1)[-1]
    filenames["zip_filepath"] = os.path.join(cwd, filenames["zip_filename"])

    tsv_matches = re.search(r"^(.+)\.(.+)\.(tsv).gz$", filenames["zip_filename"])
    filenames["filename"] = f"{tsv_matches[1]}_{tsv_matches[2]}"
    filenames["tsv_filename"] = os.path.join(filenames["filename"] + os.extsep + "tsv")
    filenames["tsv_filepath"] = os.path.join(cwd, filenames["tsv_filename"])

    filenames["csv_filename"] = os.path.join(filenames["filename"] + os.extsep + "csv")
    filenames["csv_filepath"] = os.path.join(cwd, filenames["csv_filename"])

    return filenames


def get_files(url):
    # Donwload datasets files from imdb website

    try:
        # Donwload datasets files to current directory
        retrieve = urllib.request.urlretrieve(url, os.path.join(os.getcwd(), url.rsplit("/", 1)[-1]))
        filepath = retrieve[0]
        print(f"Downloaded {filepath}")
        return filepath
    except Error as e:
        print(f"Download {filepath} fail")


def extract_file(zip_filepath, tsv_filepath):
    # Unzip and rename the tsv files
    
    try:
        with gzip.open(zip_filepath, "rb") as f_in:
            with open(tsv_filepath, "wb") as f_out:
                f_out.write(f_in.read())
                print(f"Extracted to {tsv_filepath}")
    except Error as e:
        print(f"Extract {zip_filepath} Failed")


def convert_file(filename, tsv_filepath, csv_filepath):
    # Data cleaning and convert tsv to csv file

    imdb_table = pd.read_table(tsv_filepath, sep="\t")
    
    # Data Cleaning for title_basics
    if filename == "title_basics":

        # Locate all rows with primaryTilte issues
        title_issue_df = imdb_table[imdb_table["primaryTitle"].str.contains(r".+\t.+") == True]
        
        # If rows with primaryTilte issues exit
        if title_issue_df.shape[0] > 0:
            rows_fixed = 0
            for index, row in title_issue_df.iterrows():
                values = row.values.flatten().tolist()
                
                # Split the string to two columns
                clean_titles = values[2].split("\t")
                values[2] = clean_titles[0]
                values.insert(3, clean_titles[1])

                # Removed unnecessary NaN value at the end
                values.pop()

                # Replace the row in the table
                imdb_table[imdb_table["tconst"] == values[0]] = values
                rows_fixed += 1
            print(f"Fixed {rows_fixed} row")
        
    # Export csv
    try:
        imdb_table.to_csv(csv_filepath, index=False)
        print(f"Converted {tsv_filepath} to {csv_filepath}")
    except:
        print(f"Convert {tsv_filepath} failed")


def get_df(filename, filepath):
    # Read csv and more data cleaning

    df = pd.read_csv(filepath, index_col=False)
    temp_df = df

    # Data cleaning for "title_basics.csv"
    if filename == "title_basics":
        
        # Replace \N with None
        temp_df.replace(r"\\N", None, regex=True, inplace=True)

        # Replace NaN with None
        df = temp_df.where(pd.notnull(temp_df), None)
    return df


def config_connect():
    # Configurating the MySQL connection

    # Login credentials
    config = {
        "host": "localhost",
        "user": input("Username: "),
        "password": getpass("Enter password: "),
    }
    return config


def connect_database(cursor):
    try:
        cursor.execute(f"USE {DB_NAME}")
        return True
    except Error as e:
        if e.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database {DB_NAME} does not exists.")
            create_database(cursor)
        else:
            print(e)


def create_database(cursor):
    try:
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"{DB_NAME} database created.")
        connect_database(cursor)
        return True
    except Error as e:
        print(f"Create {DB_NAME} Failed")
        sys.exit(1)


def create_table(cursor, table):
    create_query = TABLES[table]
    try:
        cursor.execute(create_query)
        print(f"Created table {table}")
        return True
    except Error as e:
        if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print(f"{table} already exist")
        else:
            print(e)
        return False


def insert_row(cursor, table, df):
    counter_inserted = 0
    counter_skipped = 0
    for index, row in df.iterrows():
        try:
            insert_data = tuple(row)
            cursor.execute(INSERTS[table], insert_data)
            counter_inserted += 1
        except Error as e:
            print(f"Skipped row {index}: {insert_data}")
            print(e)
            counter_skipped += 1
    return counter_inserted, counter_skipped


# *Start MySQL server on your computer before running the script

def main():
    imdb_df = {}

    # Set configuration for MySQL server
    print("Configuring MySQL server...")
    config = config_connect()

    for url in URLS:
        
        # Get filenames and path from url
        filenames = get_filenames(url)

        # Download the dataset file
        if os.path.exists(filenames["zip_filepath"]):
            print(f"{filenames['zip_filename']} already existed")
        else:
            print("Downloading datasets from IMDB...")
            get_files(url)

        # Extract the tsv from zip file
        if os.path.exists(filenames["tsv_filepath"]):
            print(f"{filenames['tsv_filename']} already existed")
        else:
            print(f"Extracting files from {filenames['zip_filename']}...")
            extract_file(filenames["zip_filepath"], filenames["tsv_filepath"])
        
        # Convert tsv file to csv file
        if os.path.exists(filenames["csv_filepath"]):#
            print(f"{filenames['csv_filename']} already existed")#
        else:
            print(f"Converting {filenames['tsv_filename']} to {filenames['csv_filename']}...")
            convert_file(filenames["filename"], filenames["tsv_filepath"], filenames["csv_filepath"])#
        
        # Read csv file
        print(f"Reading {filenames['csv_filename']}...")
        imdb_df[filenames["filename"]] = get_df(filenames["filename"], filenames["csv_filepath"])

        
    try: 
        with connect(**config) as cnx: 
            with cnx.cursor() as cursor:
                print(f"Connecting to {DB_NAME} database ...")
                connect_database(cursor)
                print(f"Connected to {DB_NAME} database successfully")
                for table in TABLES:
                    create_table(cursor, table)
                    print(f"Inserting rows to {table}...")
                    inserted, skipped = insert_row(cursor, table, imdb_df[table])
                    print(f"{table}: Inserted {inserted} rows, skipped {skipped} rows")
                    cnx.commit()
                print("Finished!")
    except Error as e:
        print(e)
        sys.exit(1)

if __name__ == "__main__":
    main()
