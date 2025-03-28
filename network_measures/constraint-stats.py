# Databricks notebook source
# required for reading from storage
permissions = "fulldata"
#permissions = 'default'
project_name = 'proj_1050_authorship_networks'

# COMMAND ----------

from pyspark.sql.functions import col, lit, sum as spark_sum
from pyspark.sql import functions as func
from pyspark.sql.functions import round
from graphframes import GraphFrame

# Function to get filtered edges for a given focal vertices DataFrame
def get_filtered_edges(edges, focal_vertices):
    # Get neighbors from node1
    neighbors_from_node1 = edges.join(focal_vertices, edges.node1 == focal_vertices.id, "inner") \
        .select(col("node2").alias("neighbor_id"))

    # Get neighbors from node2
    neighbors_from_node2 = edges.join(focal_vertices, edges.node2 == focal_vertices.id, "inner") \
        .select(col("node1").alias("neighbor_id"))

    # Combine and distinct all neighbors
    all_neighbors = neighbors_from_node1.union(neighbors_from_node2).distinct()

    # Combine focal vertices with their neighbors
    focal_and_neighbors = focal_vertices.withColumnRenamed("id", "neighbor_id") \
        .union(all_neighbors).distinct()

    # Filter edges to include only those where either endpoint is a focal vertex or its neighbor
    filtered_edges = edges.join(focal_and_neighbors, edges.node1 == focal_and_neighbors.neighbor_id, 'inner') \
        .select(edges.node1, edges.node2, edges.weight) \
        .union(
            edges.join(focal_and_neighbors, edges.node2 == focal_and_neighbors.neighbor_id, 'inner') \
            .select(edges.node1, edges.node2, edges.weight)
        ).distinct()

    # Reverse edges to handle undirected graph
    reverse_edges = filtered_edges.select(
        col('node2').alias('node1'),
        col('node1').alias('node2'),
        col('weight')
    )

    return filtered_edges.union(reverse_edges)

# COMMAND ----------

# Function to compute the overall constraint for focal vertices only using pre-filtered edges
def compute_overall_constraint(focal_vertices, filtered_edges):
    # Step 1: Sum the weights for each ego (focal vertex)
    ego_total_ties = filtered_edges.groupBy('node1').agg(spark_sum('weight').alias('total_tie_strength'))

    # Join the total tie strength to calculate the proportion p_{ij}
    edges_with_proportion = filtered_edges.alias('edges') \
        .join(ego_total_ties.alias('totals'), col('edges.node1') == col('totals.node1'), 'inner') \
        .withColumn('p_ij', col('edges.weight') / col('totals.total_tie_strength')) \
        .select('edges.node1', 'edges.node2', 'p_ij')

    # Rename edge columns to 'src' and 'dst' for GraphFrame compatibility
    edges_with_proportion = edges_with_proportion.withColumnRenamed("node1", "src").withColumnRenamed("node2", "dst")

    # Create a vertices DataFrame with 'id' column
    vertices = ego_total_ties.withColumnRenamed('node1', 'id')

    # Create a GraphFrame (make sure edges have src and dst, and vertices have id)
    graph = GraphFrame(vertices, edges_with_proportion)

    paths = graph.find("(ego)-[e1]->(q); (q)-[e2]->(alter)") \
        .filter("ego.id != alter.id") \
        .filter("ego.id != q.id") \
        .filter("q.id != alter.id")

    indirect_paths = paths.withColumn(
        'indirect_p_ij', 
        col('e1.p_ij') * col('e2.p_ij')
    ).select(
        col('ego.id').alias('ego'),
        col('alter.id').alias('alter'),
        'indirect_p_ij'
    )

    # Now, let's combine the direct and indirect proportions
    direct_proportions = edges_with_proportion.select(
        col('src').alias('ego'),
        col('dst').alias('alter'),
        col('p_ij').alias('direct_p_ij')
    )

    combined_ties = direct_proportions.union(
        indirect_paths.select('ego', 'alter', col('indirect_p_ij').alias('direct_p_ij'))
    )

    total_proportions = combined_ties.join(
        edges_with_proportion,
        (combined_ties.ego == col('src')) & (combined_ties.alter == col('dst')),  # Ensure the join happens between correct columns
        'inner'
    ).groupBy('ego', 'alter').agg(
        spark_sum('direct_p_ij').alias('total_p_ij')  # Sum of direct and indirect p_ij for each ego-alter pair
    )

    # Compute the square of the total proportion for each ego-alter pair
    squared_proportions = total_proportions.withColumn(
        'squared_p_ij', col('total_p_ij') ** 2
    )

    # Sum the squared proportions for each ego (focal vertex) to compute the overall constraint
    constraint_df = squared_proportions.groupBy('ego').agg(
        spark_sum('squared_p_ij').alias('overall_constraint')
    )

    # Round the overall constraint to 4 decimal places
    constraint_df = constraint_df.withColumn('overall_constraint', round(col('overall_constraint'), 4))

    # Filter to include only focal vertices in the result
    focal_constraint_df = constraint_df.join(focal_vertices, constraint_df.ego == focal_vertices.id, 'inner') \
        .select(focal_vertices.id, 'overall_constraint')

    return focal_constraint_df

# COMMAND ----------

# Read edges and vertices from parquet files
edges = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/edges-1418-srcDst.parquet"
)

vertices = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/vertices-1418-auidYear.parquet"
)

vertices = vertices.withColumnRenamed("auid", "id")

edges = edges.select(
    col('src').alias('node1'),
    col('dst').alias('node2'),
    col('value').alias('weight')
)

# Filter vertices where `is_focal` is True
focals = vertices.filter(col("is_focal") == True).select("id")
focals.count()

# COMMAND ----------

stats = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-1418.parquet",
)
stats = stats.select("auid", "degree")
stats = stats.withColumnRenamed("auid", "id")
stats.count()

# COMMAND ----------

constraint = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalConstraint-1418-idConstraint.parquet",
)
constraint.count()

# COMMAND ----------

from pyspark.sql import functions as func
from pyspark.sql import Window

missing_data = focals.join(constraint, on="id", how="left_anti")
stats_focals = stats.join(missing_data, on='id', how='inner')
stats_focals.count()


# COMMAND ----------

sorted_df = stats_focals.orderBy(func.col('degree').asc())

# Show the sorted DataFrame
display(sorted_df)

# COMMAND ----------

missing_data = stats_focals.filter(func.col('degree') <= 250)
missing_data = missing_data.filter(func.col('degree') > 0)
missing_data = missing_data.drop('degree')
missing_data.count()

# COMMAND ----------

from pyspark.sql import functions as func
from pyspark.sql import Window

# Add a row number column ordered by the `id` column
focals_with_index = missing_data.withColumn(
    "index", func.row_number().over(Window.orderBy("id")) - 1
)

# Calculate the number of rows per split
total_rows = focals_with_index.count()
num_splits = 100
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

filtered_edges = get_filtered_edges(edges, missing_data)
filtered_edges.count()

# COMMAND ----------

overall_constraint_df = compute_overall_constraint(missing_data, filtered_edges)

# Define the file path
file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalConstraint-1418-idConstraint.parquet"

# Write the DataFrame in append mode
overall_constraint_df.write.mode("append").parquet(file_path)

# COMMAND ----------

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalConstraint-1418-idConstraint.parquet"

for i in range(1, 19):
    key = f"df{i}"
    if key in split_dfs:
        filtered_edges = get_filtered_edges(edges, split_dfs[key])
        overall_constraint_df = compute_overall_constraint(split_dfs[key], filtered_edges)
        overall_constraint_df.write.mode("append").parquet(file_path)