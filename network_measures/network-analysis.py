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
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/edges-0913.parquet",
)

# vertices are about 100mb uncompressed
vertices = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/vertices-0913.parquet",
)

# COMMAND ----------

## Calculate first-order degree centrality
The next code chunk 

# COMMAND ----------

"""
Get degrees
"""
from graphframes import *

# https://graphframes.github.io/graphframes/docs/_site/api/python/genindex.html
gf = GraphFrame(v=vertices, e=edges)

focals = vertices.filter(func.col("is_focal") == True)

focal_degrees = gf.degrees.alias("d").join(focals.alias("f"), func.col("d.id") == func.col("f.id")).selectExpr("d.id as auid", "degree")


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
    .unionByName(edges.drop("relationship"))
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

END_YEAR = 2013

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
        "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/focal-pub-counts-by-year.parquet",
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
            second_order_collab_mean_degree.alias("s"),
            func.col("s.auid") == func.col("a.auid"),
            "left",
        )
        .selectExpr("a.*", "second_order_collab_mean_degree")
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
        f"s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-0913.parquet",
        recurse=True,
    )
    stats.write.parquet(
        f"s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-0913.parquet"
    )

    return stats


stats = merge_statistics_and_save()