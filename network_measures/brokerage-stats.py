# Databricks notebook source
# required for reading from storage
permissions = "fulldata"
#permissions = 'default'
project_name = 'proj_1050_authorship_networks'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from graphframes import GraphFrame

# Create a Spark session
spark = SparkSession.builder \
    .appName("Open Triads for Subset") \
    .getOrCreate()

def compute_open_triads(g: GraphFrame, filtered_focals):
    # Step 1: Find neighbors for each focal node
    neighbors = g.edges.join(filtered_focals, g.edges.src == filtered_focals.id) \
        .groupBy("src").agg(F.collect_set("dst").alias("neighbors"))

    neighbor_counts = neighbors.withColumn("n", F.size("neighbors"))

    # Step 2: Find pairs of neighbors for each focal node
    exploded_neighbors = neighbors.withColumn("neighbor", F.explode("neighbors"))

    neighbor_pairs = exploded_neighbors.alias("n1").join(
        exploded_neighbors.alias("n2"),
        (F.col("n1.src") == F.col("n2.src")) & (F.col("n1.neighbor") < F.col("n2.neighbor"))
    ).select(F.col("n1.src").alias("focal_node"), 
             F.col("n1.neighbor").alias("neighbor1"), 
             F.col("n2.neighbor").alias("neighbor2"))

    # Step 3: Check if there's an edge between the two neighbors, i.e., filter out closed triads
    open_triads = neighbor_pairs.join(g.edges.alias("e"),
        (F.col("e.src") == F.col("neighbor1")) & (F.col("e.dst") == F.col("neighbor2")),
        how="left_anti")
    
    # Step 4: Count the number of open triads for each focal node
    open_triads_count = open_triads.groupBy("focal_node").agg(F.count("*").alias("open_triads_count"))

    normalized_open_triads_count = neighbor_counts.join(open_triads_count, neighbor_counts.src == open_triads_count.focal_node, "left") \
        .withColumn("open_triads_per_neighbor", 
            F.round((2 * F.col("open_triads_count")) / (F.col("n") * (F.col("n") - 1)), 2)) \
        .select("src", "open_triads_per_neighbor")
    
    # Return the DataFrame with the open triad count for each focal node
    return open_triads_count, normalized_open_triads_count

# COMMAND ----------

def get_open_triads_df(filtered_focals, open_triads_count):

    # Join with the focal nodes to include nodes with 0 open triads
    open_triads_df = filtered_focals.join(open_triads_count, filtered_focals.id == open_triads_count.focal_node, how="left") \
        .select("id", F.coalesce("open_triads_count", F.lit(0)).alias("open_triads_count"))
    
    # Return the DataFrame with the open triad count for each focal node
    return open_triads_df

# COMMAND ----------

def get_normalized_open_triads_df(filtered_focals, normalized_open_triads_count):

    # Join with the focal nodes to include nodes with 0 open triads
    normalized_open_triads_df = filtered_focals.join(normalized_open_triads_count, filtered_focals.id == normalized_open_triads_count.src, how="left") \
        .select("id", F.coalesce("open_triads_per_neighbor", F.lit(0)).alias("open_triads_per_neighbor"))
    
    # Return the DataFrame with the open triad count for each focal node
    return normalized_open_triads_df

# COMMAND ----------

def compute_statistics(open_triads_df):
    stats_df = open_triads_df.agg(
        F.mean('open_triads_count').alias('mean'),
        F.expr('percentile(open_triads_count, 0.5)').alias('median'),  # Median
        F.max('open_triads_count').alias('max'),
        F.min('open_triads_count').alias('min'),
        F.stddev('open_triads_count').alias('std_dev')
    )
    return stats_df

# COMMAND ----------

def compute_normalized_statistics(normalized_open_triads_df):
    stats_normalized_df = normalized_open_triads_df.agg(
        F.mean('open_triads_per_neighbor').alias('mean'),
        F.expr('percentile(open_triads_per_neighbor, 0.5)').alias('median'),  # Median
        F.max('open_triads_per_neighbor').alias('max'),
        F.min('open_triads_per_neighbor').alias('min'),
        F.stddev('open_triads_per_neighbor').alias('std_dev')
    )
    return stats_normalized_df

# COMMAND ----------

# Read edges from the parquet file
edges = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/edges-1516-srcDst.parquet"
)

# Read vertices from the parquet file
vertices = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/vertices-1516-auidYear.parquet"
)

vertices = vertices.withColumnRenamed("auid", "id")

bidirectional = (edges.withColumn("_src", F.col("dst"))
    .withColumn("_dst", F.col("src"))
    .selectExpr("_src as src", "_dst as dst")
    .unionByName(edges.drop("value"))
)

# Create the graph with undirected edges
g = GraphFrame(vertices, bidirectional)

# Filter focal vertices (change this filter to get different sets of focal nodes)
focals = vertices.filter(F.col("is_focal") == True)

# COMMAND ----------

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals)

open_triads_df = get_open_triads_df(focals, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals, normalized_open_triads_count)

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1516-idOpenTriadsCount.parquet/"

# Write the DataFrame in append mode
brokerage_df.write.mode("overwrite").parquet(file_path)

# COMMAND ----------

from pyspark.sql import functions as func
from pyspark.sql import Window

# Add a row number column ordered by the `id` column
focals_with_index = focals.withColumn(
    "index", func.row_number().over(Window.orderBy("id")) - 1
)

# Calculate the number of rows per split
total_rows = focals_with_index.count()
num_splits = 20
split_size = total_rows // num_splits

# Create a dictionary to store the split DataFrames
split_dfs = {}

# Loop to split the DataFrame based on the index
for i in range(num_splits):
    start_index = i * split_size
    # Ensure the last split includes any remaining rows
    end_index = (i + 1) * split_size if i < num_splits - 1 else total_rows
    
    split_dfs[f"df{i + 1}"] = focals_with_index.filter(
        (func.col("index") >= start_index) & (func.col("index") < end_index)
    ).drop("index")

# COMMAND ----------

open_triads_count, normalized_open_triads_count = compute_open_triads(g, split_dfs["df20"])

open_triads_df = get_open_triads_df(split_dfs["df20"], open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(split_dfs["df20"], normalized_open_triads_count)

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1418-idOpenTriadsCount.parquet/"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_female_ca_medi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_ca_medi)

open_triads_df = get_open_triads_df(focals_female_ca_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_ca_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1418-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1418-idOpenTriadsCount.parquet"

# Load the DataFrame from the S3 file
df = spark.read.parquet(file_path)

# Drop duplicates based on the 'id' column
df = df.dropDuplicates(["id"])

# Save the updated DataFrame back to the S3 file
df.write.mode("overwrite").parquet(file_path)

# COMMAND ----------

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1418-idOpenTriadsCount.parquet"

# Load the DataFrame from the S3 file
df = spark.read.parquet(file_path)
df.count()

# COMMAND ----------

focals_male_ca_medi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_ca_medi)

open_triads_df = get_open_triads_df(focals_male_ca_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_ca_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1418-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_female_ca_bioc = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_ca_bioc)

open_triads_df = get_open_triads_df(focals_female_ca_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_ca_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_male_ca_bioc = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_ca_bioc)

open_triads_df = get_open_triads_df(focals_male_ca_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_ca_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_female_ca_engi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_ca_engi)

open_triads_df = get_open_triads_df(focals_female_ca_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_ca_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_male_ca_engi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_ca_engi)

open_triads_df = get_open_triads_df(focals_male_ca_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_ca_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_female_ca_busi_econ = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_ca_busi_econ)

open_triads_df = get_open_triads_df(focals_female_ca_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_ca_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_male_ca_busi_econ = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Canada") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_ca_busi_econ)

open_triads_df = get_open_triads_df(focals_male_ca_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_ca_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Write the DataFrame in append mode
brokerage_df.write.mode("append").parquet(file_path)

# COMMAND ----------

focals_female_us_medi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_us_medi)

open_triads_df = get_open_triads_df(focals_female_us_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_us_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

brokerage_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')

# COMMAND ----------

focals_male_us_medi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_us_medi)

open_triads_df = get_open_triads_df(focals_male_us_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_us_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_us_bioc = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_us_bioc)

open_triads_df = get_open_triads_df(focals_female_us_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_us_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_us_bioc = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_us_bioc)

open_triads_df = get_open_triads_df(focals_male_us_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_us_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_us_engi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_us_engi)

open_triads_df = get_open_triads_df(focals_female_us_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_us_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_us_engi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_us_engi)

open_triads_df = get_open_triads_df(focals_male_us_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_us_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_us_busi_econ = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_us_busi_econ)

open_triads_df = get_open_triads_df(focals_female_us_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_us_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_us_busi_econ = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "United States") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_us_busi_econ)

open_triads_df = get_open_triads_df(focals_male_us_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_us_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_eu_medi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_eu_medi)

open_triads_df = get_open_triads_df(focals_female_eu_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_eu_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_eu_medi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_eu_medi)

open_triads_df = get_open_triads_df(focals_male_eu_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_eu_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_eu_bioc = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_eu_bioc)

open_triads_df = get_open_triads_df(focals_female_eu_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_eu_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_eu_bioc = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_eu_bioc)

open_triads_df = get_open_triads_df(focals_male_eu_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_eu_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_eu_engi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_eu_engi)

open_triads_df = get_open_triads_df(focals_female_eu_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_eu_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_eu_engi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_eu_engi)

open_triads_df = get_open_triads_df(focals_male_eu_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_eu_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_eu_busi_econ = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_eu_busi_econ)

open_triads_df = get_open_triads_df(focals_female_eu_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_eu_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_eu_busi_econ = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "EU") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_eu_busi_econ)

open_triads_df = get_open_triads_df(focals_male_eu_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_eu_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_japan_medi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_japan_medi)

open_triads_df = get_open_triads_df(focals_female_japan_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_japan_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_japan_medi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_japan_medi)

open_triads_df = get_open_triads_df(focals_male_japan_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_japan_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_japan_bioc = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_japan_bioc)

open_triads_df = get_open_triads_df(focals_female_japan_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_japan_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_japan_bioc = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_japan_bioc)

open_triads_df = get_open_triads_df(focals_male_japan_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_japan_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_japan_engi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_japan_engi)

open_triads_df = get_open_triads_df(focals_female_japan_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_japan_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_japan_engi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_japan_engi)

open_triads_df = get_open_triads_df(focals_male_japan_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_japan_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_japan_busi_econ = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_japan_busi_econ)

open_triads_df = get_open_triads_df(focals_female_japan_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_japan_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_japan_busi_econ = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Japan") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_japan_busi_econ)

open_triads_df = get_open_triads_df(focals_male_japan_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_japan_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_brazil_medi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_brazil_medi)

open_triads_df = get_open_triads_df(focals_female_brazil_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_brazil_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_brazil_medi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "MEDI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_brazil_medi)

open_triads_df = get_open_triads_df(focals_male_brazil_medi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_brazil_medi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_brazil_bioc = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_brazil_bioc)

open_triads_df = get_open_triads_df(focals_female_brazil_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_brazil_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_brazil_bioc = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "BIOC")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_brazil_bioc)

open_triads_df = get_open_triads_df(focals_male_brazil_bioc, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_brazil_bioc, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_brazil_engi = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_brazil_engi)

open_triads_df = get_open_triads_df(focals_female_brazil_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_brazil_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_brazil_engi = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "ENGI")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_brazil_engi)

open_triads_df = get_open_triads_df(focals_male_brazil_engi, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_brazil_engi, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_female_brazil_busi_econ = focals.filter(
    (F.col("gender") == "female") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_female_brazil_busi_econ)

open_triads_df = get_open_triads_df(focals_female_brazil_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_female_brazil_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

focals_male_brazil_busi_econ = focals.filter(
    (F.col("gender") == "male") &
    F.array_contains(F.col("countries"), "Brazil") &
    F.array_contains(F.col("subject_areas"), "BUSI_ECON")
)

open_triads_count, normalized_open_triads_count = compute_open_triads(g, focals_male_brazil_busi_econ)

open_triads_df = get_open_triads_df(focals_male_brazil_busi_econ, open_triads_count)

normalized_open_triads_df = get_normalized_open_triads_df(focals_male_brazil_busi_econ, normalized_open_triads_count)

statistics_df = compute_statistics(open_triads_df)

normalized_statistics_df = compute_normalized_statistics(normalized_open_triads_df)

statistics_df.show()

normalized_statistics_df.show()

# COMMAND ----------

combine_df = open_triads_df.join(normalized_open_triads_df, on='id', how='inner')
brokerage_df = brokerage_df.union(combine_df).dropDuplicates()

# COMMAND ----------

dbutils.fs.rm(
    f"s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet",
    recurse=True,
)

brokerage_df.write.parquet(
    f"s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"
)

# COMMAND ----------

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Read the existing Parquet file
df = spark.read.parquet(file_path)

# Drop duplicates based on 'id', keeping only the first occurrence
deduped_df = df.dropDuplicates(["id"])

# Write the deduplicated DataFrame back to the same Parquet file in overwrite mode
deduped_df.write.mode("overwrite").parquet(file_path)

# COMMAND ----------

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-1419-idOpenTriadsCount.parquet"

# Read the existing Parquet file
df = spark.read.parquet(file_path)
df.count()