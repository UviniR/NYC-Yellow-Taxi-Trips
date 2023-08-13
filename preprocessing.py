
# Import necessary libraries

# To initiate a spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, date_format, when, round

# Create a SparkSession
spark = SparkSession.builder.appName("taxi").getOrCreate()

# Read the CSV file into a dataframe
# Change the file path to fit your project
df = spark.read.csv("gs://bucket_uvini/taxi.csv", header=True, inferSchema=True)

# observe the dataframe
df.show(5)

# observe the data types
df.printSchema()

# Total number of records
record_count = df.count()
print("Total number of records: ", record_count)

# Delete unwanted columns
df = df.drop('store_and_fwd_flag', 'tpep_dropoff_datetime', 'VendorID')

# Replace 'tpep_pickup_datetime' with date portion
df = df.withColumn('pickup_date', date_format('tpep_pickup_datetime', 'yyyy-MM-dd')).drop('tpep_pickup_datetime')

# check for duplicates
duplicate_count = df.count() - df.dropDuplicates().count()

if duplicate_count > 0:
    # Remove duplicates
    df = df.dropDuplicates()
    print(f"Removed {duplicate_count} duplicates.")
else:
    print("No duplicates.")

# Replace values in RateCodeID column
df = df.withColumn('RateCodeID', when(df['RateCodeID'] == '1', 'Standard rate')
                              .when(df['RateCodeID'] == '2', 'JFK')
                              .when(df['RateCodeID'] == '3', 'Newark')
                              .when(df['RateCodeID'] == '4', 'Nassau')
                              .when(df['RateCodeID'] == '5', 'Negotiated fare')
                              .when(df['RateCodeID'] == '6', 'Group ride')
                              .otherwise(None))

# Replace values in Payment_type column
df = df.withColumn('Payment_type', when(df['Payment_type'] == '1', 'Credit card')
                              .when(df['Payment_type'] == '2', 'Cash')
                              .when(df['Payment_type'] == '3', 'No charge')
                              .when(df['Payment_type'] == '4', 'Dispute')
                              .when(df['Payment_type'] == '5', 'Unknown')
                              .when(df['Payment_type'] == '6', 'Voided trip')
                              .otherwise(None))

# check for null values
null_counts = df.select([sum(col(column).isNull().cast("int")).alias(column) for column in df.columns])
null_counts.show()

# Drop records with null values
df = df.dropna()

# Total number of records after dropping nulls
record_count = df.count()
print("Total number of records: ", record_count)

# Filter to include only records from New York pickup locations
df = df.filter(
    (df.pickup_latitude.between(40.4961, 40.9155)) &
    (df.pickup_longitude.between(-74.2556, -73.7004))
)


# Filter dropoff latitude or longitude values equal to 0
df = df.filter(
    (df.dropoff_latitude != 0) &
    (df.dropoff_longitude != 0.0)
)

# Total number of records after filtering
record_count = df.count()
print("Total number of records: ", record_count)


# View statistics of each variable
variable_stats = df.describe()
variable_stats.show()

# Columns for outlier removal
columns = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']

# Iterate over each column
for x in columns:
    # Lower and upper quantiles
    quantiles = df.approxQuantile(x, [0.25, 0.75], 0.05)

    # IQR
    iqr = quantiles[1] - quantiles[0]

    # Lower and upper bounds for outlier removal
    lower_bound = quantiles[0] - 1.5 * iqr
    upper_bound = quantiles[1] + 1.5 * iqr

    # Remove outliers
    df = df.filter((col(x) >= lower_bound) & (col(x) <= upper_bound))

# Total number of records after treating outliers
record_count = df.count()
print("Total number of records: ", record_count)

# Add new column other_charges
df = df.withColumn('other_charges', round(col('extra') + col('mta_tax') + col('tip_amount') + col('tolls_amount') + col('improvement_surcharge'), 2))

# Repartition the dataframe to a single partition
df = df.repartition(1)

# Output path for the CSV file
# Change the file path to fit your project
output_path = "gs://bucket_uvini/preprocessed"

# Save as a CSV file
df.write.csv(output_path, header=True, mode="overwrite")