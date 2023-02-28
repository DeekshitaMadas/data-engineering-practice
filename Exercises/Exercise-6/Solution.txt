from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# create SparkSession
spark = SparkSession.builder.appName('Exercise6').getOrCreate()

# set path to data folder
path = 'Exercises/Exercise-6/data/'

# set path to reports folder
report_path = 'Exercises/Exercise-6/reports/'

# define schema for the data
schema = StructType([
    StructField('trip_id', IntegerType(), True),
    StructField('start_time', StringType(), True),
    StructField('end_time', StringType(), True),
    StructField('bikeid', IntegerType(), True),
    StructField('tripduration', FloatType(), True),
    StructField('from_station_id', IntegerType(), True),
    StructField('from_station_name', StringType(), True),
    StructField('to_station_id', IntegerType(), True),
    StructField('to_station_name', StringType(), True),
    StructField('usertype', StringType(), True),
    StructField('gender', StringType(), True),
    StructField('birthyear', IntegerType(), True)
])

# function to read zip'd csv file and return a dataframe
def read_zip_csv(filename):
    df = spark.read.format("csv").option("header", "true").schema(schema).load("zip://" + os.path.abspath(filename))
    return df

# function to write dataframe to csv file
def write_to_csv(df, filename):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(report_path + filename)

# Question 1: What is the average trip duration per day?
def avg_trip_duration_per_day():
    df = read_zip_csv(path + "Divvy_Trips_2019_Q4.zip")
    df = df.withColumn('day', substring(col('start_time'), 1, 10))
    result = df.groupBy('day').agg(avg('tripduration').alias('avg_trip_duration'))
    write_to_csv(result, "avg_trip_duration_per_day.csv")

# Question 2: How many trips were taken each day?
def num_trips_per_day():
    df = read_zip_csv(path + "Divvy_Trips_2019_Q4.zip")
    df = df.withColumn('day', substring(col('start_time'), 1, 10))
    result = df.groupBy('day').count()
    write_to_csv(result, "num_trips_per_day.csv")

# Question 3: What was the most popular starting trip station for each month?
def most_popular_starting_station_per_month():
    df = read_zip_csv(path + "Divvy_Trips_2019_Q4.zip")
    df = df.withColumn('month', substring(col('start_time'), 1, 7))
    result = df.groupBy('month', 'from_station_name').count().orderBy('month', desc('count')).groupBy('month').agg(first('from_station_name').alias('most_popular_starting_station'))
    write_to_csv(result, "most_popular_starting_station_per_month.csv")

# Question 4: What were the top 3 trip stations each day for the last two weeks?
def top_3_trip_stations_last_2_weeks():
    df = read_zip_csv(path + "Divvy_Trips_2019_Q4.zip")
    df = df.withColumn('day', substring(col('start_time'), 1, 10))
    df = df.filter(col('start_time') >= '2019-12-15')
    result = df.groupBy('
-----

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def load_data(spark, file_path):
    """
    Load the data from the csv file at the given file_path.
    """
    df = spark.read.format('csv').options(header=True, inferSchema=True).load(file_path)
    return df

def preprocess_data(df):
    """
    Preprocess the data to add columns for the day and month of the trip start time.
    """
    df = df.withColumn('start_time', to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss'))
    df = df.withColumn('day', date_format('start_time', 'yyyy-MM-dd'))
    df = df.withColumn('month', date_format('start_time', 'yyyy-MM'))
    return df

def top_trip_stations_last_two_weeks(df):
    """
    Get the top 3 trip stations for each day in the last two weeks.
    """
    two_weeks_ago = datetime.now() - timedelta(days=14)
    two_weeks_ago_str = two_weeks_ago.strftime('%Y-%m-%d')
    top_trip_stations = (df.filter(col('start_time') >= two_weeks_ago_str)
                         .groupBy('day', 'from_station_name')
                         .agg(count('*').alias('num_trips'))
                         .orderBy(col('day').asc(), col('num_trips').desc())
                        )
    window_spec = Window.partitionBy('day').orderBy(col('num_trips').desc())
    top_trip_stations = top_trip_stations.withColumn('rank', rank().over(window_spec))
    top_trip_stations = top_trip_stations.filter(col('rank') <= 3).drop('rank')
    return top_trip_stations

def males_vs_females(df):
    """
    Calculate the average trip duration for males and females.
    """
    durations = (df.groupBy('gender')
                 .agg(avg('tripduration').alias('avg_trip_duration'))
                 .orderBy(col('avg_trip_duration').desc())
                )
    return durations

def top_ages_longest_shortest_trips(df):
    """
    Calculate the top 10 ages of those that take the longest and shortest trips.
    """
    longest_trips = (df.groupBy('birthyear')
                     .agg(max('tripduration').alias('max_trip_duration'))
                     .orderBy(col('max_trip_duration').desc())
                     .limit(10)
                    )
    shortest_trips = (df.groupBy('birthyear')
                      .agg(min('tripduration').alias('min_trip_duration'))
                      .orderBy(col('min_trip_duration').asc())
                      .limit(10)
                     )
    return longest_trips, shortest_trips

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Exercise6').getOrCreate()

    file_path = 'data/*.zip'
    df = load_data(spark, file_path)
    df = preprocess_data(df)

    # Get the top 3 trip stations for each day in the last two weeks
    top_trip_stations = top_trip_stations_last_two_weeks(df)
    top_trip_stations.write.mode('overwrite').csv('reports/top_trip_stations_last_two_weeks.csv', header=True)

    # Calculate the average trip duration for males and females
    durations = males_vs_females(df)
    durations.write.mode('overwrite').csv('reports/males_vs_females.csv', header=True)

    # Calculate the top 10 ages of those that take the
