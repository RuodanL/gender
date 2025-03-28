# Databricks notebook source
# required for reading from storage
permissions = "fulldata"
#permissions = 'default'
project_name = 'proj_1050_authorship_networks'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load network
# MAGIC The first step is to load the network using the saved edges and vertices files. These files are created in the notebook entitled "create_network." This network is for focal authors from 2009-2013. 

# COMMAND ----------

"""
load vertices and edges (full set) from storage
"""
from pyspark.sql import functions as func

# edges are about 3gb uncompressed
edges = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/edges-1718-srcDst.parquet",
)

# vertices are about 100mb uncompressed
vertices = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/vertices-1718-auidYear.parquet",
)

# COMMAND ----------

focals = vertices.filter(func.col("is_focal") == True)
focals.count()

# COMMAND ----------

# Check for duplicated rows
# Step 1: Group by 'auid' and count
duplicates = (
    focals.groupBy("auid")
    .count()
    .filter(func.col("count") > 1)
)

# Step 2: Join with original DataFrame to fetch all duplicate rows
duplicate_rows = focals.join(duplicates.select("auid"), on="auid", how="inner")

display(duplicate_rows)

# COMMAND ----------

from pyspark.sql import functions as func

# Define the lists of values to filter
countries_list = ["EU", "United States", "Japan", "Brazil", "Canada"]
subjects_list = ["MEDI", "BIOC", "ENGI", "BUSI_ECON"]

# Filter the DataFrame
focals_region_subject = focals.filter(
    (func.array_contains(func.col("countries"), countries_list[0]) |
     func.array_contains(func.col("countries"), countries_list[1]) |
     func.array_contains(func.col("countries"), countries_list[2]) |
     func.array_contains(func.col("countries"), countries_list[3]) |
     func.array_contains(func.col("countries"), countries_list[4])) &
    (func.array_contains(func.col("subject_areas"), subjects_list[0]) |
     func.array_contains(func.col("subject_areas"), subjects_list[1]) |
     func.array_contains(func.col("subject_areas"), subjects_list[2]) |
     func.array_contains(func.col("subject_areas"), subjects_list[3]))
)
focals_region_subject.count()

# COMMAND ----------

## Calculate first-order degree centrality
#The next code chunk 

# COMMAND ----------

"""
Get degrees
"""
from graphframes import *

# Rename 'auid' to 'id' in the vertices DataFrame
vertices = vertices.withColumnRenamed("auid", "id")

# https://graphframes.github.io/graphframes/docs/_site/api/python/genindex.html
gf = GraphFrame(v=vertices, e=edges)

focals = vertices.filter(func.col("is_focal") == True)

focal_degrees = gf.degrees.alias("d").join(focals.alias("f"), func.col("d.id") == func.col("f.id")).selectExpr("d.id as auid", "degree")


# COMMAND ----------

focals_not_in_degrees = focals.join(
    gf.degrees, focals["id"] == gf.degrees["id"], "left_anti"
).select("id", "gender")
print(f"Focal vertices without edges: {focals_not_in_degrees.count()}")


# COMMAND ----------

focal_pubs_by_year = spark.read.format("parquet").load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalPubCountsWide-1418-auidYear.parquet",
)
focals_not_in_degrees = focals_not_in_degrees.withColumnRenamed("id", "auid")
focals_not_in_degrees = focals_not_in_degrees.join(focal_pubs_by_year, on="auid", how='inner')
focals_not_in_degrees.count()

# COMMAND ----------

stats = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-1418.parquet"
)
stats.count()

# COMMAND ----------

from pyspark.sql import functions as func

# Step 1: Identify columns in df2 that are not in df1
missing_columns = set(stats.columns) - set(focals_not_in_degrees.columns)

# Step 2: Add missing columns to df1 with a default value of 0
for col in missing_columns:
    focals_not_in_degrees = focals_not_in_degrees.withColumn(col, func.lit(0))

# Step 3: Ensure column order matches between df1 and df2
focals_not_in_degrees = focals_not_in_degrees.select(stats.columns)

# Step 4: Perform the union operation
result_df = focals_not_in_degrees.union(stats)

# Show the result
display(result_df)


# COMMAND ----------

result_df.count()

# COMMAND ----------

file_path = "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-1418.parquet"

# Write the DataFrame in append mode
result_df.write.mode("overwrite").parquet(file_path)

# COMMAND ----------

"""
Get gender pct of every focal author collaborator
"""
from pyspark.sql import functions as func


# union the edge list with src-dst reversed so that each edge is represented twice, once in each direction
# since our network is undirected, we need to do this to aggregate author-wise statistics
# since a->b will add b to a's aggregate numbers, but we'll also need b->a to get a into b's
bidirectional = (edges.withColumn("_src", func.col("dst"))
    .withColumn("_dst", func.col("src"))
    .selectExpr("_src as src", "_dst as dst")
    .unionByName(edges.drop("value"))
)

gender_collab_pct = (
    bidirectional.alias("e")
    .join(focals.alias("f"), func.col("f.id") == func.col("e.src"))
    .selectExpr("src as auid", "dst")
    .join(vertices.alias("v"), func.col("v.id") == func.col("e.dst"))
    .selectExpr("auid", "dst", "v.gender as dst_gender")
    .groupBy(func.col("auid"))
    .pivot("dst_gender")
    .count()
    .na.fill(0)
    .withColumn(
        "total_collabs",
        func.col("null") + func.col("female") + func.col("male") + func.col("unknown"),
    )
    .withColumn(
        "pct_male_first_order", func.round(func.col("male") / func.col("total_collabs"), 3) * 100
    )
    .withColumn(
        "pct_female_first_order",
        func.round(func.col("female") / func.col("total_collabs"), 3) * 100,
    )
)

# COMMAND ----------

# Gender homophily -- take the above and join on vertices to get auid src, then add column that counts number of collabs with same gender or NA, then get pct
gender_homophily = (
    gender_collab_pct.alias("g")
    .join(vertices.alias("v"), func.col("v.id") == func.col("g.auid"))
    .selectExpr("g.auid", "v.gender", "male", "female", "total_collabs")
    # b/c these are focal authors, all should have a male or female gender
    .withColumn(
        "homophilic_collabs",
        func.when(func.col("gender") == "male",
        func.col("male"))
        .when(func.col("gender") == "female", func.col("female"))
    )
    .withColumn("pct_homophilic_collabs", func.col("homophilic_collabs") / func.col("total_collabs"))
    .selectExpr("auid","gender","pct_homophilic_collabs")
)

# COMMAND ----------

# get average years of experience per collaborator for each focal author

END_YEAR = 2018

mean_publication_tenure = (
    bidirectional
    .alias("e")
    .selectExpr("src", "dst")
    .join(focals.alias("f"), func.col("f.id") == func.col("e.src"))
    .join(vertices.alias("v"), func.col("v.id") == func.col("e.dst"))
    .selectExpr("src as auid", "dst", "v.first_pub_year as dst_first_pub_year")
    .withColumn("end_year", func.lit(END_YEAR))
    .withColumn("dst_tenure", END_YEAR - func.col("dst_first_pub_year"))
    .groupBy("auid")
    .agg(func.round(func.mean(func.col("dst_tenure")), 2).alias("mean_collab_tenure"))
)

# COMMAND ----------

# get average pub count per collaborator for each focal author

mean_pub_count = (
    bidirectional
    .alias("e")
    .selectExpr("src", "dst")
    .join(focals.alias("f"), func.col("f.id") == func.col("e.src"))
    .join(vertices.alias("v"), func.col("v.id") == func.col("e.dst"))
    .selectExpr("src as auid", "dst", "v.pub_count as dst_pub_count")
    .groupBy("auid")
    .agg(func.round(func.mean(func.col("dst_pub_count")), 2).alias("mean_collab_pub_count"))
)

# COMMAND ----------

# second-order degree centrality is the average number of edges for each first-order collaborator

focal_edges = (
    bidirectional
        .alias("e")
        .join(focals.alias("f"), func.col("f.id") == func.col("e.src"))
        .selectExpr("f.id as src", "e.dst as dst")
        .alias("e")
        # the above means that all src's are focal, and thus all dsts are their collaborators   
)

# create an edgelist representing triads focal -> foc -> soc
focal_second_order_collabs = (
    focal_edges.alias("e")
        # now we want to join back to the doubled edge list n on e.dst = n.src, but only when e.src <> n.dst
        # since we want to avoid loops e.g., that a -> b -> c isn't actually just a -> b -> a
        .join(bidirectional.alias("n"), (func.col("e.dst") == func.col("n.src")) & (func.col("n.dst") != func.col("e.src")))
        .selectExpr("e.src as focal_src", "e.dst as fo_dst", "n.dst as so_dst")
    )

# get the ids of the second order collabs so we can calculate their degree
focal_collabs_unique_ids = focal_second_order_collabs.selectExpr("so_dst as auid").distinct()

second_order_collab_counts = (
    focal_collabs_unique_ids.alias("f")
        .join(bidirectional.alias("b"), (func.col("f.auid") == func.col("b.src")))
        .select("b.*")
        .distinct()
        .groupBy("src")
        .agg(func.countDistinct("dst").alias("collab_count"))
)

second_order_collab_mean_degree = (
       second_order_collab_counts.alias("s")
        .join(focal_second_order_collabs.alias("f"), (func.col("f.so_dst") == func.col("s.src")))
        .selectExpr("f.focal_src as src","f.so_dst as dst","collab_count")
        .distinct()
        .groupBy("src")
        .agg(func.mean("collab_count").alias("second_order_collab_mean_degree"))
        .selectExpr("src as auid", "second_order_collab_mean_degree")
)

# COMMAND ----------

# Interdisciplinary reach
from pyspark.sql.window import Window


vertices_exploded_subject = vertices.withColumn(
    "subject", func.explode(func.col("subject_areas"))
)

vertices_exploded_country = vertices.withColumn(
    "country", func.explode(func.col("countries"))
)

vertices_exploded_country_no_eu = vertices.withColumn(
    "country", func.explode(func.col("countries_wo_eu"))
)

windowSpecAgg = Window.partitionBy("src")

def get_reach(exploded_vertices, compare_col_name, target_col_name):
    """
    Get the proportion of first-order collaborators who who have a different `compare_col` than focal author.
    This is meant to compare fields that are in arrays (countries, subjects)
    """

    alias = f"ex.{compare_col_name}"

    return (
        bidirectional.alias("e")
        # first annotate src and dst with subject areas
        .join(
            exploded_vertices.alias("ex"),
            func.col("e.src") == func.col("ex.id"),
        )
        .selectExpr("e.src as src", f"{alias} as src_comp_col", "e.dst as dst")
        .join(
            exploded_vertices.alias("ex"), func.col("dst") == func.col("ex.id")
        )
        .selectExpr("src", "src_comp_col", "dst", f"{alias} as dst_comp_col")
        .withColumn(
            "is_same",
            func.when(func.col("src_comp_col") == func.col("dst_comp_col"), 1).otherwise(0),
        )
        .groupBy("src", "dst")
        .agg(func.sum(func.col("is_same")).alias("same_count"))
        # since we exploded the target column, we might have a single collaboration with several matches
        # we want to make sure that we count each collaboration only once
        .withColumn(
            "is_same_comp_col", func.when(func.col("same_count") > 0, 1).otherwise(0)
        )
        .withColumn("total_collabs", func.count(func.col("dst")).over(windowSpecAgg))
        .withColumn(
            "total_same_collabs", func.sum(func.col("is_same_comp_col")).over(windowSpecAgg)
        )
        .withColumn(
            target_col_name,
            1 - (func.col("total_same_collabs") / func.col("total_collabs")),
        )
        .selectExpr("src as auid", target_col_name)
        .distinct()
    )


interdisciplinary_reach = get_reach(vertices_exploded_subject, "subject", "interdisciplinary_reach").orderBy("auid")

international_reach_with_eu = get_reach(vertices_exploded_country, "countries", "international_reach_with_eu").orderBy("auid")

international_reach_no_eu = get_reach(vertices_exploded_country_no_eu, "countries_wo_eu", "international_reach_no_eu").orderBy("auid")



# COMMAND ----------

def merge_statistics_and_save():
    """
    Merge statistics and save.
    """
    # first read in by-year publications, then the rest
    focal_pubs_by_year = spark.read.format("parquet").load(
        "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalPubCountsWide-1718-auidYear.parquet",
    )

    stats = (
        focal_degrees.alias("a")
        .join(
            gender_collab_pct.alias("gcp"),
            func.col("gcp.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr(
            "a.auid", "degree", "pct_male_first_order", "pct_female_first_order"
        )
        .alias("a")
        .join(
            gender_homophily.alias("g"),
            func.col("g.auid") == func.col("a.auid"),
            "left",
        )
        .select("a.*", "gender", "pct_homophilic_collabs")
        .alias("a")
        .join(
            mean_publication_tenure.alias("pt"),
            func.col("pt.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr("a.*", "mean_collab_tenure")
        .alias("a")
        .join(
            mean_pub_count.alias("pc"),
            func.col("pc.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr("a.*", "mean_collab_pub_count")
        .alias("a")
        .join(
            interdisciplinary_reach.alias("i"),
            func.col("i.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr("a.*", "interdisciplinary_reach")
        .alias("a")
        .join(
            international_reach_with_eu.alias("i"),
            func.col("i.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr("a.*", "international_reach_with_eu")
        .alias("a")
        .join(
            international_reach_no_eu.alias("i"),
            func.col("i.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr("a.*", "international_reach_no_eu")
        .alias("a")
        .join(
            focal_pubs_by_year.withColumnRenamed("auid", "f_auid").alias("f"),
            func.col("f_auid") == func.col("a.auid"),
            "left",
        )
        .drop("f_auid")
    )

    dbutils.fs.rm(
        f"s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-1718.parquet",
        recurse=True,
    )
    stats.write.parquet(
        f"s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-1718.parquet"
    )

    return stats


stats = merge_statistics_and_save()

# COMMAND ----------

from pyspark.sql import functions as F

stats = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-0913.parquet",
)

# COMMAND ----------

stats.count()

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

year_columns = ['2014', '2015', '2016']
stats = stats.withColumn("sum_2014_2016", sum([coalesce(col(year), lit(0)) for year in year_columns]))

display(stats)

# COMMAND ----------

vertices = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/vertices-0913-auidYear.parquet"
)

# Filter vertices where `is_focal` is True
focals = vertices.filter(col("is_focal") == True).select("auid", "subject_areas", "countries")

stats_combine_df = stats_esize_brokerage.join(focals, on='auid', how='inner')
display(stats_combine_df)

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as func

stats_combine = stats_combine_df.filter(
    func.array_contains(func.col("countries"), "EU") &
    func.array_contains(func.col("subject_areas"), "BUSI_ECON")
)

# Convert to Pandas
pandas_df = stats_combine.toPandas()

# Encode gender
pandas_df["gender"] = pandas_df["gender"].map({"female": 0, "male": 1})

# Split into train and test sets
from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(pandas_df, test_size=0.3, random_state=42)

# Fit Negative Binomial model
import statsmodels.api as sm
from patsy import dmatrices

formula = "sum_2014_2016 ~ gender"
y_train, X_train = dmatrices(formula, train_df, return_type="dataframe")
nb_model = sm.GLM(y_train, X_train, family=sm.families.NegativeBinomial()).fit()

# Summary
print(nb_model.summary())

# Predict on test set
y_test, X_test = dmatrices(formula, test_df, return_type="dataframe")
predictions = nb_model.predict(X_test)

# Compare predictions
test_df["predicted_productivity"] = predictions
print(test_df[["sum_2014_2016", "predicted_productivity"]])



# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error

mae = mean_absolute_error(test_df["sum_2014_2016"], test_df["predicted_productivity"])
rmse = mean_squared_error(test_df["sum_2014_2016"], test_df["predicted_productivity"], squared=False)

print(f"Mean Absolute Error (MAE): {mae}")
print(f"Root Mean Squared Error (RMSE): {rmse}")


# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as func

stats_combine = stats_constraint.filter(
    func.array_contains(func.col("countries"), "Japan") &
    func.array_contains(func.col("subject_areas"), "MEDI")
)

# Convert to Pandas
pandas_df = stats_combine.toPandas()

# Encode gender
pandas_df["gender"] = pandas_df["gender"].map({"female": 0, "male": 1})

# Split into train and test sets
from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(pandas_df, test_size=0.3, random_state=42)

# Fit Negative Binomial model
import statsmodels.api as sm
from patsy import dmatrices

formula = "sum_2014_2016 ~ gender + overall_constraint"
#formula = "sum_2014_2016 ~ gender * (efficiency + open_triads_per_neighbor + pct_homophilic_collabs + sum_2009_2013)"

# Drop nulls
train_df = train_df.dropna(subset=["gender", "sum_2014_2016", "overall_constraint"])

# Normalize predictors
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()

train_df[["overall_constraint"]] = scaler.fit_transform(
    train_df[["overall_constraint"]]
)

import statsmodels.api as sm
from patsy import dmatrices

# Prepare matrices for training
y_train, X_train = dmatrices(formula, train_df, return_type="dataframe")

# Fit the model
nb_model = sm.GLM(y_train, X_train, family=sm.families.NegativeBinomial()).fit()

# Model summary
print(nb_model.summary())



# COMMAND ----------

test_df[["overall_constraint"]] = scaler.transform(
    test_df[["overall_constraint"]]
)

y_test, X_test = dmatrices(formula, test_df, return_type="dataframe")
predictions = nb_model.predict(X_test)

# Compare actual vs predicted
test_df["predicted_productivity"] = predictions
display(test_df[["sum_2014_2016", "predicted_productivity"]])

# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error

mae = mean_absolute_error(test_df["sum_2014_2016"], test_df["predicted_productivity"])
rmse = mean_squared_error(test_df["sum_2014_2016"], test_df["predicted_productivity"], squared=False)

print(f"Mean Absolute Error (MAE): {mae}")
print(f"Root Mean Squared Error (RMSE): {rmse}")

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as func

stats_combine = stats_constraint.filter(
    func.array_contains(func.col("countries"), "Japan") &
    func.array_contains(func.col("subject_areas"), "MEDI")
)

# Convert to Pandas
pandas_df = stats_combine.toPandas()

# Encode gender
pandas_df["gender"] = pandas_df["gender"].map({"female": 0, "male": 1})

# Split into train and test sets
from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(pandas_df, test_size=0.3, random_state=42)

# Fit Negative Binomial model
import statsmodels.api as sm
from patsy import dmatrices

formula = "sum_2014_2016 ~ gender + efficiency + open_triads_per_neighbor + pct_homophilic_collabs + overall_constraint"
#formula = "sum_2014_2016 ~ gender * (efficiency + open_triads_per_neighbor + pct_homophilic_collabs + sum_2009_2013)"

# Drop nulls
train_df = train_df.dropna(subset=["gender", "sum_2014_2016", "efficiency", "open_triads_per_neighbor", "pct_homophilic_collabs", "overall_constraint"])

# Normalize predictors
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()

train_df[["efficiency", "open_triads_per_neighbor", "pct_homophilic_collabs", "overall_constraint"]] = scaler.fit_transform(
    train_df[["efficiency", "open_triads_per_neighbor", "pct_homophilic_collabs", "overall_constraint"]]
)

import statsmodels.api as sm
from patsy import dmatrices

# Prepare matrices for training
y_train, X_train = dmatrices(formula, train_df, return_type="dataframe")

# Fit the model
nb_model = sm.GLM(y_train, X_train, family=sm.families.NegativeBinomial()).fit()

# Model summary
print(nb_model.summary())



# COMMAND ----------

test_df[["efficiency", "open_triads_per_neighbor", "pct_homophilic_collabs", "overall_constraint"]] = scaler.transform(
    test_df[["efficiency", "open_triads_per_neighbor", "pct_homophilic_collabs", "overall_constraint"]]
)

y_test, X_test = dmatrices(formula, test_df, return_type="dataframe")
predictions = nb_model.predict(X_test)

# Compare actual vs predicted
test_df["predicted_productivity"] = predictions
display(test_df[["sum_2014_2016", "predicted_productivity"]])


# COMMAND ----------

from sklearn.metrics import mean_squared_error, mean_absolute_error

mae = mean_absolute_error(test_df["sum_2014_2016"], test_df["predicted_productivity"])
rmse = mean_squared_error(test_df["sum_2014_2016"], test_df["predicted_productivity"], squared=False)

print(f"Mean Absolute Error (MAE): {mae}")
print(f"Root Mean Squared Error (RMSE): {rmse}")


# COMMAND ----------

import matplotlib.pyplot as plt

plt.scatter(test_df["sum_2014_2016"], test_df["predicted_productivity"], alpha=0.6)
plt.xlabel("Actual Productivity")
plt.ylabel("Predicted Productivity")
plt.title("Actual vs Predicted Productivity")
plt.show()


# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as func

stats_combine = stats_combine_df.filter(
    func.array_contains(func.col("countries"), "EU") &
    func.array_contains(func.col("subject_areas"), "MEDI")
)

# Convert to Pandas
pandas_df = stats_combine.toPandas()

# Encode gender
pandas_df["gender"] = pandas_df["gender"].map({"female": 0, "male": 1})

# Split into train and test sets
from sklearn.model_selection import train_test_split
train_df, test_df = train_test_split(pandas_df, test_size=0.3, random_state=42)

# Fit Negative Binomial model
import statsmodels.api as sm
from patsy import dmatrices

formula = "sum_2014_2016 ~ gender * (efficiency + open_triads_per_neighbor + pct_homophilic_collabs)"

# Prepare design matrices
y_train, X_train = dmatrices(formula, train_df, return_type="dataframe")

# Fit the Negative Binomial model
nb_model = sm.GLM(y_train, X_train, family=sm.families.NegativeBinomial()).fit()

# Display summary
print(nb_model.summary())


# COMMAND ----------

y_test, X_test = dmatrices(formula, test_df, return_type="dataframe")
predictions = nb_model.predict(X_test)

# Add predictions to test dataframe
test_df["predicted_productivity"] = predictions
display(test_df[["sum_2014_2016", "predicted_productivity"]])

# COMMAND ----------

mae = mean_absolute_error(test_df["sum_2014_2016"], test_df["predicted_productivity"])
rmse = mean_squared_error(test_df["sum_2014_2016"], test_df["predicted_productivity"], squared=False)

print(f"MAE: {mae}, RMSE: {rmse}")


# COMMAND ----------

import matplotlib.pyplot as plt

plt.scatter(test_df["sum_2014_2016"], test_df["predicted_productivity"], alpha=0.6)
plt.xlabel("Actual Productivity")
plt.ylabel("Predicted Productivity")
plt.title("Actual vs Predicted Productivity")
plt.show()


# COMMAND ----------

year_columns = ['2009', '2010', '2011', '2012', '2013']
stats = stats.withColumn("sum_2009_2013", sum([coalesce(col(year), lit(0)) for year in year_columns]))

display(stats)

# COMMAND ----------

zero_pubs = stats.filter(col("sum_2014_2016") == 0)
zero_pubs.count()

# COMMAND ----------

stats.count()

# COMMAND ----------

constraint_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalOverallConstraint-0913-idOverallconstraint.parquet",
)

# COMMAND ----------

constraint_df = constraint_df.dropDuplicates(["id"])
constraint_df = constraint_df.withColumnRenamed("id", "auid")

# COMMAND ----------

stats_constraint = stats_combine_df.join(constraint_df, on='auid', how='inner')
display(stats_constraint)

# COMMAND ----------

stats = stats.select("auid", "degree", "pct_male_first_order", "pct_female_first_order", "gender", "pct_homophilic_collabs", "mean_collab_tenure", "mean_collab_pub_count", "second_order_collab_mean_degree", "interdisciplinary_reach", "international_reach_with_eu", "international_reach_no_eu", "sum_2014_2016")
stats_constraint = stats.join(constraint_df, on='auid', how='inner')
display(stats_constraint)

# COMMAND ----------

import pandas as pd

df = stats_constraint.toPandas()

# Calculate mean and standard deviation
stats = df["overall_constraint"].agg(["mean", "std", "min", "max"])

# Calculate correlations with "sum_2014_2016"
correlations = df["overall_constraint"].corr(df["sum_2014_2016"])

# Display results
print(stats)
print("\nCorrelations with sum_2014_2016:")
display(correlations)

# COMMAND ----------

female_stats_constraint = stats_constraint.filter(col("gender") == "female")
male_stats_constraint = stats_constraint.filter(col("gender") == "male")

# COMMAND ----------

df = female_stats_constraint.toPandas()

# Calculate mean and standard deviation
stats = df["overall_constraint"].agg(["mean", "std", "min", "max"])

# Calculate correlations with "sum_2014_2016"
correlations = df["overall_constraint"].corr(df["sum_2014_2016"])

# Display results
print(stats)
print("\nCorrelations with sum_2014_2016:")
display(correlations)

# COMMAND ----------

df = male_stats_constraint.toPandas()

# Calculate mean and standard deviation
stats = df["overall_constraint"].agg(["mean", "std", "min", "max"])

# Calculate correlations with "sum_2014_2016"
correlations = df["overall_constraint"].corr(df["sum_2014_2016"])

# Display results
print(stats)
print("\nCorrelations with sum_2014_2016:")
display(correlations)

# COMMAND ----------

esize_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalEsizeEfficiency-0913-idEsizeEfficiency.parquet",
)

# COMMAND ----------

brokerage_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-0913-idOpenTriadsCount.parquet",
)

# COMMAND ----------

esize_brokerage_df = esize_df.join(brokerage_df, on='id', how='inner')
esize_brokerage_df = esize_brokerage_df.withColumnRenamed("id", "auid")

# COMMAND ----------

stats = stats.select("auid", "degree", "pct_male_first_order", "pct_female_first_order", "gender", "pct_homophilic_collabs", "mean_collab_tenure", "mean_collab_pub_count", "second_order_collab_mean_degree", "interdisciplinary_reach", "international_reach_with_eu", "international_reach_no_eu", "sum_2009_2013", "sum_2014_2016")
stats_esize_brokerage = stats.join(esize_brokerage_df, on='auid', how='inner')
display(stats_esize_brokerage)

# COMMAND ----------

stats_esize_brokerage.count()

# COMMAND ----------

import pandas as pd

df = stats_esize_brokerage.toPandas()

# Excluding "auid" and "gender" columns
columns_to_include = df.columns.difference(["auid", "gender"])

# Calculate mean and standard deviation
stats = df[columns_to_include].agg(["mean", "std"])

# Calculate correlations with "sum_2014_2016"
correlations = df[columns_to_include].corrwith(df["sum_2014_2016"])

# Display results
print("Mean:")
display(stats.loc["mean"])
print("Standard Deviation:")
display(stats.loc["std"])
print("\nCorrelations with sum_2014_2016:")
display(correlations)


# COMMAND ----------

female_stats_esize_brokerage = stats_esize_brokerage.filter(col("gender") == "female")
male_stats_esize_brokerage = stats_esize_brokerage.filter(col("gender") == "male")

# COMMAND ----------

df = female_stats_esize_brokerage.toPandas()

# Excluding "auid" and "gender" columns
columns_to_include = df.columns.difference(["auid", "gender"])

# Calculate mean and standard deviation
stats = df[columns_to_include].agg(["mean", "std"])

# Calculate correlations with "sum_2014_2016"
correlations = df[columns_to_include].corrwith(df["sum_2014_2016"])

# Display results
print("Mean:")
display(stats.loc["mean"])
print("Standard Deviation:")
display(stats.loc["std"])
print("\nCorrelations with sum_2014_2016:")
display(correlations)


# COMMAND ----------

df = male_stats_esize_brokerage.toPandas()

# Excluding "auid" and "gender" columns
columns_to_include = df.columns.difference(["auid", "gender"])

# Calculate mean and standard deviation
stats = df[columns_to_include].agg(["mean", "std"])

# Calculate correlations with "sum_2014_2016"
correlations = df[columns_to_include].corrwith(df["sum_2014_2016"])

# Display results
print("Mean:")
display(stats.loc["mean"])
print("Standard Deviation:")
display(stats.loc["std"])
print("\nCorrelations with sum_2014_2016:")
display(correlations)


# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

np.random.seed(42)
df = stats_esize_brokerage.toPandas()

# Randomly sample 1000 rows
df = df.sample(n=min(1000, len(df)), random_state=42)

# Create the scatter plot with a regression line and confidence interval
plt.figure(figsize=(8, 6))
sns.regplot(
    x='sum_2009_2013', 
    y='sum_2014_2016', 
    data=df, 
    scatter_kws={'s': 10},  # Adjust the size of the scatter points
    line_kws={'color': 'red'},  # Color of the regression line
    ci=95,  # Confidence interval (default is 95%)
    robust=True  # Make the regression robust to outliers
)

# Add titles and labels
plt.title('Scatter Plot of Publication Counts', fontsize=14)
plt.xlabel('Publication count (2009-2013)', fontsize=12)
plt.ylabel('Publication count (2014-2016)', fontsize=12)

# Show the plot
plt.show()

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Example dataframe
np.random.seed(42)
df = stats_esize_brokerage.toPandas()

# Randomly sample 1000 rows
df = df.sample(n=min(1000, len(df)), random_state=42)

# Create the scatter plot with a regression line and confidence interval
plt.figure(figsize=(8, 6))
sns.regplot(
    x='open_triads_per_neighbor', 
    y='sum_2014_2016', 
    data=df, 
    scatter_kws={'s': 10},  # Adjust the size of the scatter points
    line_kws={'color': 'red'},  # Color of the regression line
    ci=95,  # Confidence interval (default is 95%)
    robust=True  # Make the regression robust to outliers
)

# Add titles and labels
plt.title('Scatter Plot with Confidence Interval', fontsize=14)
plt.xlabel('Normalized Open Triads Count', fontsize=12)
plt.ylabel('Publication count (2014-2016)', fontsize=12)

# Show the plot
plt.show()

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Example dataframe
np.random.seed(42)
df = stats_esize_brokerage.toPandas()

# Randomly sample 1000 rows
df = df.sample(n=min(1000, len(df)), random_state=42)

# Create the scatter plot with a regression line and confidence interval
plt.figure(figsize=(8, 6))
sns.regplot(
    x='efficiency', 
    y='sum_2014_2016', 
    data=df, 
    scatter_kws={'s': 10},  # Adjust the size of the scatter points
    line_kws={'color': 'red'},  # Color of the regression line
    ci=95,  # Confidence interval (default is 95%)
    robust=True  # Make the regression robust to outliers
)

# Add titles and labels
plt.title('Scatter Plot with Confidence Interval', fontsize=14)
plt.xlabel('Efficiency', fontsize=12)
plt.ylabel('Publication count (2014-2016)', fontsize=12)

# Show the plot
plt.show()

# COMMAND ----------

print(sampled_df[['open_triads_per_neighbor', 'sum_2014_2016']].describe())

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# Example dataframe
np.random.seed(42)
df = stats_esize_brokerage.toPandas()

# Randomly sample 1000 rows
sampled_df = df.sample(n=min(1000, len(df)), random_state=42)
sampled_df['log_effective_size'] = np.log1p(sampled_df['effective_size'])

# Plot scatter plot with linear regression
plt.figure(figsize=(10, 6))
sns.regplot(
    data=sampled_df,
    x="log_effective_size",
    y="sum_2014_2016",
    ci=95,  # Confidence interval
    scatter_kws={'alpha': 0.5},  # Make scatter points slightly transparent
    line_kws={'color': 'red'}  # Regression line color
)

# Add labels and title
plt.xlabel('Log Effective Size', fontsize=12)
plt.ylabel('Publication count (2014-2016)', fontsize=12)
plt.title('Scatter Plot with Regression Line (Effective Size vs Publication count)', fontsize=14)
plt.grid(alpha=0.3)
plt.tight_layout()

# Show the plot
plt.show()

# COMMAND ----------

print(sampled_df[['effective_size', 'sum_2014_2016']].describe())

# COMMAND ----------

sampled_df[['effective_size', 'sum_2014_2016']].hist(bins=30, figsize=(10, 4), layout=(1, 2))
plt.show()

# COMMAND ----------



# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

#pdf = PdfPages('2014-2016_pub_counts_distribution.pdf')

stats_df = stats[stats['sum_2014_2016'] <= 20]
df = stats_df.toPandas()

# Plotting the histogram
plt.figure(figsize=(8, 6))
plt.hist(df['sum_2014_2016'], bins=range(22), align='left', color='skyblue', edgecolor='black')

# Setting x-axis and y-axis labels
plt.xlabel('Publication Counts (2014-2016)', fontsize=12)
plt.ylabel('Frequency', fontsize=12)

# Adding a title
plt.title('Distribution of Publication Counts (2014-2016)', fontsize=14)

# Adjusting tick labels for better visibility
plt.xticks(range(21), fontsize=10)  # Show ticks from 0 to 10
plt.yticks(fontsize=10)

# Display the plot
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

#pdf.savefig()  # Save the current figure to the PDF
#plt.close()  # Close the figure


# COMMAND ----------

# Total number of IDs
total_ids = stats.count()

# Plotting the histogram with percentages
plt.figure(figsize=(8, 6))
counts, bins, bars = plt.hist(
    df['sum_2014_2016'], 
    bins=range(22), 
    align='left', 
    color='skyblue', 
    edgecolor='black', 
    weights=[1 / total_ids] * len(df)  # Normalize by total number of IDs
)

# Adding percentage annotations
for count, x in zip(counts, bins[:-1]):  # Iterate through counts and bin edges
    if count > 0:  # Avoid displaying annotations for empty bins
        plt.text(
            x, count + 0.005,  # Position above the bar
            f'{count * 100:.1f}',  # Convert to percentage
            ha='center', fontsize=10
        )

# Converting y-axis to percentages
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y * 100:.0f}'))

# Setting x-axis and y-axis labels
plt.xlabel('Publication Counts (2014-2016)', fontsize=12)
plt.ylabel('Percentage of Total Focals', fontsize=12)

# Adding a title
plt.title('Distribution of Publication Counts (2014-2016)', fontsize=14)

# Adjusting tick labels for better visibility
plt.xticks(range(21), fontsize=10)  # Show ticks from 0 to 20
plt.yticks(fontsize=10)

# Display the plot
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

#pdf.savefig()  # Save the current figure to the PDF
#plt.close()  # Close the figure

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

stats_df = stats[stats['sum_2014_2016'] <= 20]
df = stats_df.toPandas()

# Separate data by gender
female_data = df[df['gender'] == 'female']['sum_2014_2016']
male_data = df[df['gender'] == 'male']['sum_2014_2016']

# Total counts per gender for normalization
total_female = stats.filter(col('gender') == 'female').count()
total_male = stats.filter(col('gender') == 'male').count()

# Creating bins for histogram
bins = range(22)

# Calculating histograms as percentages
female_counts, _ = np.histogram(female_data, bins=bins)
male_counts, _ = np.histogram(male_data, bins=bins)

female_percentages = female_counts / total_female * 100 if total_female > 0 else np.zeros(len(bins) - 1)
male_percentages = male_counts / total_male * 100 if total_male > 0 else np.zeros(len(bins) - 1)

# Plotting
x = np.arange(len(female_percentages))  # Positions for the x-axis

plt.figure(figsize=(12, 6))
bar_width = 0.4

# Female bar plot
plt.bar(x - bar_width / 2, female_percentages, width=bar_width, color='salmon', edgecolor='black', label='Female')

# Male bar plot
plt.bar(x + bar_width / 2, male_percentages, width=bar_width, color='skyblue', edgecolor='black', label='Male')

# Adding percentage annotations
for i in range(len(female_percentages)):
    # Female annotations
    plt.text(
        x[i] - bar_width / 2, 
        female_percentages[i] + 1, 
        f'{female_percentages[i]:.1f}', 
        ha='center', 
        fontsize=10
    )
    # Male annotations
    # Adjust position to avoid overlap
    male_y_offset = 2 if abs(female_percentages[i] - male_percentages[i]) < 0.7 else 1
    plt.text(
        x[i] + bar_width / 2, 
        male_percentages[i] + male_y_offset, 
        f'{male_percentages[i]:.1f}', 
        ha='center', 
        fontsize=10
    )

# Formatting the plot
plt.xlabel('Publication Counts (2014-2016)', fontsize=12)
plt.ylabel('Percentage within Gender', fontsize=12)
plt.title('Distribution of Publication Counts by Gender (2014-2016)', fontsize=14)
plt.xticks(x, labels=np.arange(21), fontsize=10)  # X-axis labels from 0 to 20
plt.yticks(fontsize=10)
plt.legend(fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Display the plot
plt.tight_layout()
plt.show()

#pdf.savefig()  # Save the current figure to the PDF
#plt.close()  # Close the figure
#pdf.close()
