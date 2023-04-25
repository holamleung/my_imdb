# Building and Querying IMDB Database

This program downloads IMDb datasets from IMDb website and imports them into a MySQL database for easy querying and analysis. The IMDb datasets include information about movies and TV shows such as title, type, release year, ratings, and more.

## Prerequisites
In order to run this program, you need to have the following installed on your system:

Python 3
Pandas
MySQL server
MySQL Python Connector
Jupyter Notebook

## Usage
1. Run the jupyter notebook
2. Run the cells in order
3, Configure the MySQL connection parameters in the "Prepare: Setting Parameters" section. You need to provide the MySQL host, username, password, and database name where you want to import the IMDb datasets.

## Functionality
The program performs the following tasks:

1. Downloads the IMDb datasets in gzip format from the IMDb website.
2. Extracts the TSV files from the downloaded gzip files.
3. Cleans the data in the TSV files and converts them to CSV format.
4. Imports the CSV files into the MySQL database.
5. Performs data cleaning and transformation on the "title_basics" dataset by fixing issues with the "primaryTitle" column.
6. Conects to MySQL server with username and password for establishing a connection to the MySQL server.
7. Create database, tables, and rows in MySQL
8. Querying the database 
