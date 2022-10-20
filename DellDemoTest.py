# import packages
import psycopg2
import psycopg2.extras as extras
import pandas as pd

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    print(tuples)

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()


def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df

# establishing connection
conn = psycopg2.connect(
    database="delldemo",
    user='postgres',
    password='pgpassword',
    host='127.0.0.1',
    port='5432'
)
# creating a cursor
cursor = conn.cursor()
#cursor.execute(sql)

#reading the json data from web url and storing as pandas dataframe.
url = "https://opendata.ecdc.europa.eu/covid19/nationalcasedeath/json/"
webCovidDF = pd.read_json(url)
#print(df.columns)
#print(df)

#storing column names in a list.
column_names=["country", "country_code", "continent", "population", "indicator", "weekly_count", "year_week", "cumulative_count", "source", "rate_14_day", "note"]
#retriving the existing data from postgressql and store as pandas dataframe.
postgresCovidDF = postgresql_to_dataframe(conn, "select * from covid19data", column_names)
#print(df1)

#comparing the two data frames and retrieve he unique rows.
#below webCovidDF has latest data and postgresCovidDF has existing data
updatedCovidDF = webCovidDF[~webCovidDF.year_week.isin(postgresCovidDF.year_week)]
#print(df3)
#if updatedCovidDF has more than 0 rows, then it will insert the rows into postgresSQL
if len(updatedCovidDF) > 0:
    execute_values(conn, updatedCovidDF, 'covid19data')
