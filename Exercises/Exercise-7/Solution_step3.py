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
