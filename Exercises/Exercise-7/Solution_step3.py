from pyspark.sql.functions import input_file_name, regexp_extract, to_date, when

# read the zipped csv file into a DataFrame
df = spark.read.format("csv") \
           .option("header", True) \
           .option("inferSchema", True) \
           .option("compression", "zip") \
           .load("data/hard-drive-2022-01-01-failures.csv.zip")

# add a new column with the file name
df = df.withColumn("source_file", input_file_name())

# extract the date from the file name and add it as a new column
df = df.withColumn("file_date", to_date(regexp_extract("source_file", "(\d{4}-\d{2}-\d{2})", 1)))

# add a new column called brand based on the column model
df = df.withColumn("brand", when(df.model.startswith("HGST"), "HGST")
                             .when(df.model.startswith("ST"), "Seagate")
                             .when(df.model.startswith("TOSHIBA"), "Toshiba")
                             .when(df.model.startswith("WDC"), "Western Digital")
                             .otherwise("Other"))

##aternate brand solution
# Add a new column called brand
df = df.withColumn("brand", when(split(df.model, "\s+").getItem(0).isNull(), "unknown").otherwise(split(df.model, "\s+").getItem(0)))

# Inspect a column called capacity_bytes. Create a secondary DataFrame that relates capacity_bytes to the model column, create "buckets" / "rankings" for those models with the most capacity to the least. Bring back that data as a column called storage_ranking into the main dataset.
capacity_df = df.select("model", "capacity_bytes").distinct().groupBy("model").max("capacity_bytes").orderBy("max(capacity_bytes)", ascending=False)
capacity_df = capacity_df.withColumn("storage_ranking", coalesce(capacity_df.ranking(), capacity_df.count()))
df = df.join(capacity_df.select("model", "storage_ranking"), "model", "left")

# Create a column called primary_key that is hash of columns that make a record unique in this dataset.
df = df.withColumn("primary_key", hash(df.survey_date, df.serial_number, df.model))

# Show the final DataFrame
df.show()




