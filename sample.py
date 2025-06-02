import pandas as pd # type: ignore
import mysql.connector # type: ignore
from sqlalchemy import create_engine,text # type: ignore

file_path = 'C:/Users/91949/Desktop/netflix_titles.csv'
df = pd.read_csv(file_path)
print("Successfully loaded data into pandas DataFrame.")
print(df.head(5))

db= "Netflix_DataSet"   # your target DB
# SQLAlchemy engine with PyMySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="1234"
)

cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS Netflix_DataSet")
print("Database ensured.")
cursor.close()
conn.close()
# Create SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://root:1234@localhost/{db}")

# Load DataFrame to SQL table
table_name = "netflix_titles"
df.to_sql(table_name, con=engine, if_exists='replace', index=False)
print(f"Data loaded into table '{table_name}' in database '{db}'.")



# Query the table and display results
query = text("SELECT count(*) FROM netflix_titles")  # wrap query with text()
with engine.connect() as connection:
    result_df = pd.read_sql(query, con=connection)
print(result_df.head())  # Display the first few rows

print(df[df.show_id == "s5023"])  # Example query to filter DataFrame