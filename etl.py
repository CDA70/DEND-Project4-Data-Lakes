import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Processing the song data creates the song and artist table and saves it as parquet files
        
        Parameter:
            - spark: spark session
            - input_data: s3a://udacity-dend/song_data/A/A/*/*.json
            - output_data: s3a://dend-cda/output/

        Return:
            - nil
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # SONGS Table (song_id, title, artist_id, year, duration)
    # extract columns to create songs table
    df.createOrReplaceTempView("songs_table")
    songs_table = df.select(
                    'song_id',
                    'title',
                    'artist_id',
                    'year',
                    'duration') \
                    .dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    print("start - writing songs parquet files")
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(os.path.join(output_data, 'songs_table', 'songs_table.parquet'))
    print("end - writing songs parquet files")

    # ARTISTS Table (artist_id, name, location, lattitude, longitude)
    # extract columns to create artists table
    df.createOrReplaceTempView("artists_table")
    artists_table = df.select('artist_id', 
                            'artist_name',
                            'artist_location',
                            'artist_latitude',
                            'artist_longitude') \
                        .dropDuplicates(['artist_id']) \
                        .withColumnRenamed('artist_name', 'name') \
                        .withColumnRenamed('artist_location', 'location') \
                        .withColumnRenamed('artist_latitude', 'latitude') \
                        .withColumnRenamed('artist_longitude', 'longitude') \

    # write artists table to parquet files
    print("start - writing artist parquet files")
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists_table', 'artists_table.parquet'))
    print("end - writing artist parquet files")

def process_log_data(spark, input_data, output_data):
    """
        Processing the log data creates the users, time and songplays table and saves it as parquet files
        
        Parameter:
            - spark: spark session
            - input_data: s3a://udacity-dend/log_data/*/*/*.json
            - output_data: s3a://dend-cda/output/

        Return:
            - nil
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table 
    df.createOrReplaceTempView("users_table")
    users_table = df.select('userId', 
                            'firstName',
                            'lastName',
                            'gender',
                            'level' ) \
                    .dropDuplicates(['userId']) \
                    .withColumnRenamed('userId', 'user_id') \
                    .withColumnRenamed('firstName', 'first_name') \
                    .withColumnRenamed('lastName', 'last_name')

    
    # write users table to parquet files
    print("start - writing users parquet files")
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users_table', 'users_table.parquet'))
    print("end - writing users parquet files")

    # create datetime column from original timestamp column
    
    #get_datetime = udf()
    df = df.withColumn("ts", (F.to_timestamp(df.ts/1000)))
    df = df.withColumn("dt", (F.to_date(df.ts)))
    
    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    df.createOrReplaceTempView("time_table")
    time_table = df.select('ts', 'dt')
    time_table = time_table.withColumnRenamed('ts', 'start_time') 

    time_table = time_table.withColumn('hour', hour(df.dt)) 
    time_table = time_table.withColumn('day', dayofmonth(df.dt))  
    time_table = time_table.withColumn('week', weekofyear(df.dt)) 
	time_table = time_table.withColumn('month', month(df.dt))
    time_table = time_table.withColumn('year', year(df.dt)) 
    time_table = time_table.withColumn('weekday', dayofweek(df.dt)) 
 
    
    # write time table to parquet files partitioned by year and month
    print("start - writing time parquet files")
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'time_table', 'time.parquet'))
    print("end - writing time parquet files")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/A/A/*/*.json")
    joined_df = df.join(song_df,
                        (df.song==song_df.title)&(df.artist==song_df.artist_name),
                        'inner'
                       )

    # extract columns from joined song and log datasets to create songplays table 
    songplay_table = joined_df.distinct() \
                            .withColumn('songplay_id', F.monotonically_increasing_id()) \
                            .selectExpr('songplay_id',
                                        'ts as start_time',
                                        'extract(month from ts) as month',
                                        'extract(year from ts) as year',
                                        'userId as user_id',
                                        'level',
                                        'song_id',
                                        'artist_id',
                                        'sessionId as session_id',
                                        'location',
                                        'userAgent as user_agent')

    # write songplays table to parquet files partitioned by year and month
    print("start - writing songplay parquet files")
    songplay_table.write.mode('overwrite').partitionBy('year','month').parquet(os.path.join(output_data, 'songplays_table', 'songsplay.parquet'))
    print("end - writing songplay parquet files")


def main():
    """
        main procedure to transform the json tables into parquet files in a s3 bucket
        
        Parameter:
            - nil

        Return:
            - nil
    """
    spark = create_spark_session()
    # the song data is three folders deep and stored in json files
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-cda/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
