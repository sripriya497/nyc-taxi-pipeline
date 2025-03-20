from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, hour, count, avg, round, unix_timestamp

#Initialize Spark Session
def get_spark_session():
    return SparkSession.builder.appName("NYCTaxiData").getOrCreate()

#Read data from parquet file
def read_data(spark,path):
    return spark.read.parquet(path)

#Clean data
def clean_data(df):
    df = df.fillna({'passenger_count' : 1, 'RatecodeID' : 1, 'store_and_fwd_flag' : 'N', 'congestion_surcharge' : 0.0, 'Airport_fee' : 0.0})
    #df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()
    df = df.filter((col('fare_amount') >  0) & (col('trip_distance') > 0) & (col('tpep_dropoff_datetime') > col('tpep_pickup_datetime')))
    return df

def remove_outliers(df):
  
    percentile = df.approxQuantile(["trip_distance","trip_duration","fare_amount"], [0.25, 0.50, 0.75, 0.98, 0.99], 0.01)  
    #print(percentile)
    trip_distance_98 = percentile[0][3]
    trip_duration_98 = percentile[1][3]
    fare_amount_98 = percentile[2][3]
    df = df.filter((col('trip_distance') < trip_distance_98) & (col('trip_duration') < trip_duration_98) & (col('fare_amount') < fare_amount_98))
    df.describe(['trip_duration','trip_distance','fare_amount']).show() #Summary statistics
    return df

#Aggregating data
def aggregate_data(df):
    #Adding a new column trip_duration
    df = df.withColumn('trip_duration',round((unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime'))/60,2))
    df.describe(['trip_duration','trip_distance','fare_amount']).show() #Summary statistics
    df_hourly = df.groupBy(hour('tpep_pickup_datetime').alias('hour')).agg(count('*').alias('trip_count'), avg('fare_amount').alias('avg_fare')).orderBy('hour') #Group by date and aggregate
    #df_hourly.show()
    return df

if __name__ == "__main__":
    spark = get_spark_session()
    filepath = "data/yellow_tripdata_2024-12.parquet"
    df = read_data(spark, filepath)
    df = clean_data(df)
    df = aggregate_data(df)
    df = remove_outliers(df)

    output_path = "data/clean_taxi_data.parquet"
    df.write.mode("overwrite").parquet(output_path)
    print("Data saved to parquet file", output_path)
    read_data(spark,output_path).show(5)
    spark.stop()