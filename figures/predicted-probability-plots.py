# Databricks notebook source
# required for reading from storage
permissions = "fulldata"
#permissions = 'default'
project_name = 'proj_1050_authorship_networks'

# COMMAND ----------

stats = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-0913.parquet",
)

normalized_homophily = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalNormalizedHomophily-0913-idNormalizedHomophily.parquet",
).drop('pct_homophilic_collabs', 'gender')

stats = stats.join(normalized_homophily, on='auid', how='inner')

# COMMAND ----------

stats = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/stats-0913.parquet",
)

normalized_homophily = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalNormalizedHomophily-0913-idNormalizedHomophily.parquet",
).drop('pct_homophilic_collabs', 'gender')

stats = stats.join(normalized_homophily, on='auid', how='inner')

avg_citation_4y = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalAvgCitations-0913-auidAvgCitation4yCount.parquet",
)

avg_normalized_citescore = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalAvgNormalizedCiteScore-0913-auidAvgNormalizedCiteScore.parquet",
)
stats = stats.join(avg_normalized_citescore, on='auid', how='inner')
stats.count()

# COMMAND ----------

display(stats.filter(F.col("interdisciplinary_reach").isNull()))

# COMMAND ----------

# Create a variable to measure publishing early or later
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql import functions as F

year_columns1 = ['2014', '2015', '2016']
stats = stats.withColumn("productivity_1416", sum([coalesce(col(year), lit(0)) for year in year_columns1]))

year_columns2 = ['2009', '2010', '2011', '2012', '2013']
stats = stats.withColumn("productivity_0913", sum([coalesce(col(year), lit(0)) for year in year_columns2]))

# Calculate the average year
stats = stats.withColumn(
    "weighted_sum",
    sum(F.coalesce(F.col(str(year)), F.lit(0)) * year for year in year_columns2)
).withColumn(
    "average_year",
    F.when(F.col("productivity_0913") > 0, F.col("weighted_sum") / F.col("productivity_0913"))
    .otherwise(None)
)

# Compute the difference from the start year (2009)
stats = stats.withColumn(
    "avg_year_diff",
    F.when(F.col("average_year").isNotNull(), F.col("average_year") - 2009)
)

# Drop intermediate columns if not needed
stats = stats.drop("weighted_sum", "average_year")

stats = stats.select("auid", "degree", "avg_normalized_citescore", "productivity_1416", "productivity_0913", "avg_year_diff", "gender", "pct_homophilic_collabs", "normalized_gender_homophily", "interdisciplinary_reach")

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql import functions as F

#year_columns1 = ['2014', '2015', '2016']
#stats = stats.withColumn("productivity_1416", sum([coalesce(col(year), lit(0)) for year in year_columns1]))

#year_columns2 = ['2009', '2010', '2011', '2012', '2013']
#stats = stats.withColumn("productivity_0913", sum([coalesce(col(year), lit(0)) for year in year_columns2]))

vertices = spark.read.format('parquet').load(
    "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/vertices-0913-auidYear.parquet"
)

# Filter vertices where `is_focal` is True
focals = vertices.filter(col("is_focal") == True).select("auid", "first_pub_year", "subjects_focal", "countries_focal")

# Add a new column 'years_from_first_pub' that calculates the difference from first_pub_year to 2019
focals = focals.withColumn("years_from_first_pub", 
                              F.when(col("first_pub_year").isNotNull(), 2013 - col("first_pub_year"))
                               .otherwise(None))

stats_focals = stats.join(focals, on='auid', how='inner')

# COMMAND ----------

brokerage_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-0913-idOpenTriadsCount.parquet",
)
brokerage_df = brokerage_df.withColumnRenamed("id", "auid")
combined_df = stats_focals.join(brokerage_df, on='auid', how='inner')
combined_df = combined_df.withColumnRenamed("open_triads_count", "brokerage")
combined_df = combined_df.withColumnRenamed("open_triads_per_neighbor", "normalized_brokerage")
combined_df = combined_df.withColumnRenamed("pct_homophilic_collabs", "homophily")
combined_df = combined_df.select("auid", "degree", "gender", "years_from_first_pub", "interdisciplinary_reach", "productivity_0913", "citation_4y_count", "brokerage", "normalized_brokerage", "homophily", "normalized_gender_homophily", "countries_focal", "subjects_focal")
combined_df.count()

# COMMAND ----------

from pyspark.sql import functions as F

# Filter rows where 'countries_focal' or 'subjects_focal' has multiple entries
filtered_df = combined_df.filter(
    (F.size(F.col("countries_focal")) > 1) | 
    (F.size(F.col("subjects_focal")) > 1)
)

# Show the filtered rows
filtered_df.count()


# COMMAND ----------

display(combined_df)

# COMMAND ----------

esize_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalEsizeEfficiency-0913-idEsizeEfficiency.parquet",
)
brokerage_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalBrokerage-0913-idOpenTriadsCount.parquet",
)
esize_brokerage_df = esize_df.join(brokerage_df, on='id', how='inner')

constraint_df = spark.read.format('parquet').load(
  "s3a://elsevier-fcads-icsr-userdata-ro/proj_1050_authorship_networks/FocalConstraint-0913-idConstraint.parquet",
)
esize_brokerage_constraint_df = esize_brokerage_df.join(constraint_df, on='id', how='inner')
esize_brokerage_constraint_df = esize_brokerage_constraint_df.withColumnRenamed("open_triads_count", "brokerage")
esize_brokerage_constraint_df = esize_brokerage_constraint_df.withColumnRenamed("open_triads_per_neighbor", "normalized_brokerage")
esize_brokerage_constraint_df = esize_brokerage_constraint_df.withColumnRenamed("id", "auid")
esize_brokerage_constraint_df = esize_brokerage_constraint_df.withColumnRenamed("overall_constraint", "constraint")


combined_df = stats_focals.join(esize_brokerage_constraint_df, on='auid', how='inner')
combined_df = combined_df.withColumnRenamed("pct_homophilic_collabs", "homophily")
combined_df = combined_df.select("auid", "degree", "gender", "years_from_first_pub", "interdisciplinary_reach", "avg_normalized_citescore","productivity_0913", "productivity_1416", "brokerage", "normalized_brokerage", "constraint", "homophily", "normalized_gender_homophily", "avg_year_diff", "countries_focal", "subjects_focal")
combined_df = combined_df.filter(F.col("degree") <= 1000)
combined_df = combined_df.filter(F.col("productivity_0913") > 1)
#combined_df = combined_df.filter(F.col("normalized_brokerage") > 0)
combined_df.count()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Step 2: Explode countries_focal and subjects_focal
# Create a column with all combinations of countries and subjects
df = df.withColumn("country_subject_combinations", F.expr("""
    transform(countries_focal, country -> 
        transform(subjects_focal, subject -> 
            struct(country as country, subject as subject)
        )
    )
"""))

# Flatten the combinations to one row per country-subject pair
df = df.withColumn("country_subject_combinations", F.flatten("country_subject_combinations"))
df = df.withColumn("country_subject_pair", F.explode("country_subject_combinations"))

# Extract exploded values into separate columns
df = df.withColumn("country", F.col("country_subject_pair.country"))
df = df.withColumn("subject", F.col("country_subject_pair.subject"))

# Step 3: Convert countries and subjects into one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.when(F.col("country") == country, 1).otherwise(0).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.when(F.col("subject") == subject, 1).otherwise(0).cast(IntegerType()))

# Step 4: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "avg_citation_4y", "women", "normalized_brokerage", "constraint", "normalized_gender_homophily", "degree", 
    "productivity_0913", "avg_year_diff", "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Step 2: Explode countries_focal and subjects_focal
# Create a column with all combinations of countries and subjects
df = df.withColumn("country_subject_combinations", F.expr("""
    transform(countries_focal, country -> 
        transform(subjects_focal, subject -> 
            struct(country as country, subject as subject)
        )
    )
"""))

# Flatten the combinations to one row per country-subject pair
df = df.withColumn("country_subject_combinations", F.flatten("country_subject_combinations"))
df = df.withColumn("country_subject_pair", F.explode("country_subject_combinations"))

# Extract exploded values into separate columns
df = df.withColumn("country", F.col("country_subject_pair.country"))
df = df.withColumn("subject", F.col("country_subject_pair.subject"))

# Step 3: Convert countries and subjects into one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.when(F.col("country") == country, 1).otherwise(0).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.when(F.col("subject") == subject, 1).otherwise(0).cast(IntegerType()))

# Create a dummy variable for 'normalized_brokerage'
#df = df.withColumn(
#    "dummy_brokerage",
#    F.when(F.col("normalized_brokerage") > 0, 1).otherwise(0).cast(IntegerType())
#)

# Create a dummy variable for 'productivity_0913'
#df = df.withColumn(
#    "dummy_productivity_0913",
#    F.when(F.col("productivity_0913") == 1, 1).otherwise(0).cast(IntegerType())
#)

# Step 4: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "avg_citation_4y", "women", "normalized_brokerage", "constraint", "normalized_gender_homophily", "degree", 
    "productivity_0913", "avg_year_diff", "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Step 5: Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 6: Define the regression formula with interaction terms
formula = """
avg_citation_4y ~ 
women + normalized_brokerage + constraint + normalized_gender_homophily + degree +  
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + avg_year_diff + years_from_first_pub + interdisciplinary_reach +
women * normalized_brokerage + women * constraint + women * normalized_gender_homophily + women * degree 
"""

# Fit the Negative Binomial model
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

# Fit the Negative Binomial model with robust standard errors
mod = smf.glm(
    formula=formula, 
    data=pdf, 
    family=sm.families.NegativeBinomial()
).fit(cov_type="HC0")  # Use robust covariance type

# Display Results
results = pd.DataFrame({
    "Variable": mod.params.index,
    "Coefficient": mod.params.values,
    "IRR": np.exp(mod.params.values),  # IRR is exp(coefficient)
    "P-Value": mod.pvalues.values
}).set_index("Variable")

# Add Log-Likelihood as the first row
log_likelihood = mod.llf
results = pd.DataFrame({
    "Coefficient": [log_likelihood],
    "IRR": [None],
    "P-Value": [None]
}, index=["Log-Likelihood"]).append(results)

# Round each result to 4 decimal places
results = results.round(4)

#results.to_csv('citation_model_nodrop.csv', index=True)


# COMMAND ----------

citation_mean = pdf["avg_citation_4y"].mean()
citation_std = pdf["avg_citation_4y"].std()
print(citation_mean)

# COMMAND ----------

productivity_mean = pdf["productivity_1416"].mean()
productivity_std = pdf["productivity_1416"].std()
print(productivity_mean)

# COMMAND ----------

print(citation_std)

# COMMAND ----------

#get predicted citation under different scenarios

# Calculate mean and standard deviation for normalized_brokerage and constraint
brokerage_mean = pdf["normalized_brokerage"].mean()
brokerage_std = pdf["normalized_brokerage"].std()
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Define the five scenarios
scenarios = [
    {"normalized_brokerage": brokerage_mean + brokerage_std, "constraint": constraint_mean},
    {"normalized_brokerage": brokerage_mean, "constraint": constraint_mean},
    {"normalized_brokerage": brokerage_mean - brokerage_std, "constraint": constraint_mean},
    {"normalized_brokerage": brokerage_mean, "constraint": constraint_mean + constraint_std},
    {"normalized_brokerage": brokerage_mean, "constraint": constraint_mean - constraint_std},
]

# Compute predictions for each scenario
predictions = []
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

for scenario in scenarios:
    for women in [1, 0]:  # 1 for women, 0 for men
        data = mean_values.copy()
        data["normalized_brokerage"] = scenario["normalized_brokerage"]
        data["constraint"] = scenario["constraint"]
        data["women"] = women
        predicted_value = mod.predict(pd.DataFrame([data]), linear=False)[0]
        predictions.append({
            "scenario": scenario,
            "women": "Women" if women == 1 else "Men",
            "predicted_productivity": predicted_value
        })

# Convert predictions to a DataFrame
predictions_df = pd.DataFrame(predictions)

# Display predictions
display(predictions_df)


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Step 2: Explode countries_focal and subjects_focal
# Create a column with all combinations of countries and subjects
df = df.withColumn("country_subject_combinations", F.expr("""
    transform(countries_focal, country -> 
        transform(subjects_focal, subject -> 
            struct(country as country, subject as subject)
        )
    )
"""))

# Flatten the combinations to one row per country-subject pair
df = df.withColumn("country_subject_combinations", F.flatten("country_subject_combinations"))
df = df.withColumn("country_subject_pair", F.explode("country_subject_combinations"))

# Extract exploded values into separate columns
df = df.withColumn("country", F.col("country_subject_pair.country"))
df = df.withColumn("subject", F.col("country_subject_pair.subject"))

# Step 3: Convert countries and subjects into one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.when(F.col("country") == country, 1).otherwise(0).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.when(F.col("subject") == subject, 1).otherwise(0).cast(IntegerType()))

# Create a dummy variable for 'normalized_brokerage'
#df = df.withColumn(
#    "dummy_brokerage",
#    F.when(F.col("normalized_brokerage") > 0, 1).otherwise(0).cast(IntegerType())
#)

# Create a dummy variable for 'productivity_0913'
#df = df.withColumn(
#    "dummy_productivity_0913",
#    F.when(F.col("productivity_0913") == 1, 1).otherwise(0).cast(IntegerType())
#)

# Step 4: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "productivity_1416", "avg_normalized_citescore", "women", "normalized_brokerage", "constraint", "normalized_gender_homophily", "degree", 
    "productivity_0913", "avg_year_diff", "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Step 5: Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 6: Define the regression formula with interaction terms
formula1 = """
productivity_1416 ~ 
women + normalized_brokerage + constraint + normalized_gender_homophily + degree +  
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + avg_year_diff + years_from_first_pub + interdisciplinary_reach +
women * normalized_brokerage + women * constraint + women * normalized_gender_homophily + women * degree 
"""

formula2 = """
avg_normalized_citescore ~ 
women + normalized_brokerage + constraint + normalized_gender_homophily + degree +  
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + avg_year_diff + years_from_first_pub + interdisciplinary_reach +
women * normalized_brokerage + women * constraint + women * normalized_gender_homophily + women * degree 
"""

# Fit the Negative Binomial model
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

# Fit the Negative Binomial model with robust standard errors
mod1 = smf.glm(
    formula=formula1, 
    data=pdf, 
    family=sm.families.NegativeBinomial()
).fit(cov_type="HC0")  # Use robust covariance type

mod2 = smf.glm(
    formula=formula2, 
    data=pdf, 
    family=sm.families.NegativeBinomial()
).fit(cov_type="HC0")  # Use robust covariance type


# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
brokerage_range = np.linspace(pdf["brokerage"].min(), pdf["brokerage"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for brokerage in brokerage_range:
        data = mean_values.copy()
        data["brokerage"] = brokerage
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod2.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "brokerage": brokerage,
            "predicted_citation": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

fig = plt.figure(figsize=(12, 8))

# Main subplot for scatter plot and predicted lines (plot first)
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = plt.plot(
        subset["brokerage"], 
        subset["predicted_citation"], 
        label=label, color=color, linewidth=1
    )

    # Plot confidence interval as shaded region
#    plt.fill_between(
#        subset["brokerage"], 
#        subset["lower_bound"], 
#        subset["upper_bound"], 
#        color='dimgray', alpha=0.2
#    )

plt.tight_layout()

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
brokerage_range = np.linspace(pdf["brokerage"].min(), pdf["brokerage"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for brokerage in brokerage_range:
        data = mean_values.copy()
        data["brokerage"] = brokerage
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod2.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "brokerage": brokerage,
            "predicted_citation": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=1000, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["brokerage"], 
        subset["predicted_citation"], 
        label=label, color=color, linewidth=1
    )

    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["brokerage"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color='dimgray', alpha=0.2
    )

    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["brokerage"], 
        sampled_subset["avg_citation_4y"],  
        edgecolor='black', color=color, alpha=0.5, s=10  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 41.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements, fontsize=10, frameon=False)

ax2.set_xlabel("Brokerage", fontsize=14)
ax2.set_ylabel("Predicted Citation", fontsize=14)
ax2.grid(False)

# Set x-axis tick numbers
#ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
#ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
ax2.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40])
ax2.set_yticklabels(["0", "5", "10", "15", "20", "25", "30", "35", "40"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_title("Predicted Citation vs. Brokerage", fontsize=16)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.xaxis.set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 41, 2)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)
ax3.yaxis.set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
#plt.savefig("citation_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

pred_se = np.sqrt(np.diag(mod1.cov_params()))  # Get standard errors from the model
print("Standard errors for predictions:", pred_se)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Set the inflation factor for standard errors (z-factor)
z_factor = 10  # Wider than normal CI (~1.96 for 95%)

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod1.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        # Extract standard error and manually scale it
        pred_std_error = pred_results.se_mean[0]  # Standard error of the mean
        lower = pred_mean - z_factor * pred_std_error
        upper = pred_mean + z_factor * pred_std_error
        
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_productivity": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["predicted_productivity"], 
        label=label, color=color
    )
    
    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["normalized_brokerage"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color=color, alpha=0.2
    )

    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        sampled_subset["productivity_1416"],  
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 18)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["normalized_brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Predicted Productivity_1416 vs. Normalized Brokerage")
ax2.set_xlabel("Normalized Brokerage")
ax2.set_ylabel("Predicted Productivity")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
ax2.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax2.set_yticklabels(["0", "3", "6", "9", "12", "15", "18"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)
# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 19, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod1.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_productivity": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=1000, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["predicted_productivity"], 
        label=label, color=color, linewidth=1
    )
    
    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["normalized_brokerage"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color='dimgray', alpha=0.2
    )

    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        sampled_subset["productivity_1416"],  
        edgecolor='black', color=color, alpha=0.5, s=10  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 18.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["normalized_brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements, fontsize=10, frameon=False)

ax2.set_xlabel("Normalized Brokerage", fontsize=14)
ax2.set_ylabel("Predicted Productivity", fontsize=14)
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
ax2.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax2.set_yticklabels(["0", "3", "6", "9", "12", "15", "18"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_title("Predicted Productivity vs. Normalized Brokerage", fontsize=16)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)
# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.xaxis.set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 19, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)
ax3.yaxis.set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod1.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "constraint": constraint,
            "predicted_productivity": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })

predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=1000, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["constraint"], 
        subset["predicted_productivity"], 
        label=label, color=color, linewidth=1
    )

    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["constraint"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color='dimgray', alpha=0.2
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["constraint"], 
        sampled_subset["productivity_1416"],  
        edgecolor='black', color=color, alpha=0.5, s=10  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 18.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, 1.25)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["constraint"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["constraint"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements, fontsize=10, frameon=False)

ax2.set_xlabel("Constraint", fontsize=14)
ax2.set_ylabel("Predicted Productivity", fontsize=14)
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1.0", "1.2"])
ax2.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax2.set_yticklabels(["0", "3", "6", "9", "12", "15", "18"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_title("Predicted Productivity vs. Constraint", fontsize=16)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)
# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.xaxis.set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 19, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)
ax3.yaxis.set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_constraint.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod2.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_citation": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=1000, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["predicted_citation"], 
        label=label, color=color, linewidth=1
    )

    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["normalized_brokerage"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color='dimgray', alpha=0.2
    )

    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        sampled_subset["avg_normalized_citescore"],  
        edgecolor='black', color=color, alpha=0.5, s=10  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 15.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["normalized_brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["avg_normalized_citescore"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["avg_normalized_citescore"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements, fontsize=10, frameon=False)

ax2.set_xlabel("Normalized Brokerage", fontsize=14)
ax2.set_ylabel("Predicted Normalized Citation", fontsize=14)
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
#ax2.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40])
#ax2.set_yticklabels(["0", "5", "10", "15", "20", "25", "30", "35", "40"])

ax2.set_yticks([0, 5, 10, 15])
ax2.set_yticklabels(["0", "5", "10", "15"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_title("Predicted Normalized Citation vs. Normalized Brokerage", fontsize=16)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.xaxis.set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 16, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="avg_normalized_citescore",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="avg_normalized_citescore",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)

ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)
ax3.yaxis.set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("citescore_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod2.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_citation": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=1000, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["predicted_citation"], 
        label=label, color=color, linewidth=1
    )

    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["normalized_brokerage"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color='dimgray', alpha=0.2
    )

    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        sampled_subset["avg_citation_4y"],  
        edgecolor='black', color=color, alpha=0.5, s=10  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 41.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["normalized_brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements, fontsize=10, frameon=False)

ax2.set_xlabel("Normalized Brokerage", fontsize=14)
ax2.set_ylabel("Predicted Citation", fontsize=14)
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
ax2.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40])
ax2.set_yticklabels(["0", "5", "10", "15", "20", "25", "30", "35", "40"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_title("Predicted Citation vs. Normalized Brokerage", fontsize=16)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.xaxis.set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 41, 2)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)
ax3.yaxis.set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("citation_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        # Get prediction and standard error
        pred_results = mod2.get_prediction(pd.DataFrame([data]))
        pred_mean = pred_results.predicted_mean[0]
        conf_int = pred_results.conf_int(alpha=0.05)  # 95% confidence interval
        lower, upper = conf_int[0]
        
        predictions.append({
            "constraint": constraint,
            "predicted_citation": pred_mean,
            "lower_bound": lower,
            "upper_bound": upper,
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=1000, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["constraint"], 
        subset["predicted_citation"], 
        label=label, color=color, linewidth=1
    )

    # Plot confidence interval as shaded region
    ax2.fill_between(
        subset["constraint"], 
        subset["lower_bound"], 
        subset["upper_bound"], 
        color='dimgray', alpha=0.2
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["constraint"], 
        sampled_subset["avg_citation_4y"],  
        edgecolor='black', color=color, alpha=0.5, s=10  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 41.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, 1.25)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["constraint"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["constraint"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements, fontsize=10, frameon=False)

ax2.set_xlabel("Constraint", fontsize=14)
ax2.set_ylabel("Predicted Citation", fontsize=14)
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1.0", "1.2"])
ax2.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40])
ax2.set_yticklabels(["0", "5", "10", "15", "20", "25", "30", "35", "40"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_title("Predicted Citation vs. Constraint", fontsize=16)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)
ax1.xaxis.set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 41, 2)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)
ax3.yaxis.set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("citation_constraint.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        pred = mod2.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["predicted_citation"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        sampled_subset["avg_citation_4y"],  
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 25)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["normalized_brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Predicted Citation_13 vs. Normalized Brokerage")
ax2.set_xlabel("Normalized Brokerage")
ax2.set_ylabel("Predicted Citation")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
ax2.set_yticks([0, 5, 10, 15, 20, 25])
ax2.set_yticklabels(["0", "5", "10", "15", "20", "25"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 26, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("citation_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        pred = mod2.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_xticklabels([])
ax1.set_xticks([])  # Remove x-axis ticks
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["constraint"], 
        subset["predicted_citation"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["constraint"], 
        sampled_subset["avg_citation_4y"],  
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 25)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, 1.25)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["constraint"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["constraint"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Predicted Citation_13 vs. Constraint")
ax2.set_xlabel("Constraint")
ax2.set_ylabel("Predicted Citation")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1.0", "1.2"])
ax2.set_yticks([0, 5, 10, 15, 20, 25])
ax2.set_yticklabels(["0", "5", "10", "15", "20", "25"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 26, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="avg_citation_4y",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("citation_constraint.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        pred = mod1.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["predicted_productivity"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        sampled_subset["productivity_1416"],  
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 18)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, pdf["normalized_brokerage"].max() + 0.08)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Predicted Productivity_1416 vs. Normalized Brokerage")
ax2.set_xlabel("Normalized Brokerage")
ax2.set_ylabel("Predicted Productivity")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])
ax2.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax2.set_yticklabels(["0", "3", "6", "9", "12", "15", "18"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)
# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 19, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        pred = mod1.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "orange"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = predictions_df[predictions_df["women"] == women]
    lines = ax2.plot(
        subset["constraint"], 
        subset["predicted_productivity"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["constraint"], 
        sampled_subset["productivity_1416"],  
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

ax2.set_ylim(-0.5, 18)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.08, 1.25)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["constraint"], 
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"],  
    ax=ax2, color="orange", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["constraint"], 
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["constraint"],  
    ax=ax2, color="turquoise", height=0.02, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='orange', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Predicted Productivity_1416 vs. Constraint")
ax2.set_xlabel("Constraint")
ax2.set_ylabel("Predicted Productivity")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1.0", "1.2"])
ax2.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax2.set_yticklabels(["0", "3", "6", "9", "12", "15", "18"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax1, color="orange", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.sharex(ax2)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)
# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

ax3 = fig.add_subplot(gs[1, 1])
# Plot histogram instead of KDE for discrete count data
bin_edges = np.arange(0, 26, 1)
sns.histplot(
    data=pdf[pdf["women"] == 1], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="orange", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
sns.histplot(
    data=pdf[pdf["women"] == 0], 
    y="productivity_1416",  # Swap x → y for vertical orientation
    ax=ax3, color="turquoise", alpha=0.5, bins=bin_edges, 
    element="bars", stat="density"
)
ax3.sharey(ax2)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

plt.subplots_adjust(hspace=0, wspace=0)
# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_constraint.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        log_pred = mod1.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "lightpink"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = log_predictions_df[log_predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["log_predicted_productivity"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        np.log(sampled_subset["productivity_1416"] + 1),  # Log-transform citations for consistency
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

# Ensure the y-axis stays within (0,6)
ax2.set_ylim(-0.2, 5.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.2, pdf["normalized_brokerage"].max() + 0.2)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="lightpink", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"] + 1),  
    ax=ax2, color="lightpink", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"] + 1),  
    ax=ax2, color="turquoise", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='lightpink', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Log of Predicted Productivity vs. Normalized Brokerage")
ax2.set_xlabel("Normalized Brokerage")
ax2.set_ylabel("Log(Predicted Productivity)")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="lightpink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_xticks([])  # Remove x-axis ticks
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

# Right subplot for density distribution of avg_citation_4y (plot third)
ax3 = fig.add_subplot(gs[1, 1])
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 1]["productivity_1416"] + 1), 
    ax=ax3, color="lightpink", alpha=0.5, fill=True, vertical=True
)
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 0]["productivity_1416"] + 1), 
    ax=ax3, color="turquoise", alpha=0.5, fill=True, vertical=True
)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.set_yticks([])  # Remove y-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_brokerage.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Step 1: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        log_pred = mod1.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "lightpink"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = log_predictions_df[log_predictions_df["women"] == women]
    lines = ax2.plot(
        subset["constraint"], 
        subset["log_predicted_productivity"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["constraint"], 
        np.log(sampled_subset["productivity_1416"] + 1),  # Log-transform citations for consistency
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

# Ensure the y-axis stays within (0,6)
ax2.set_ylim(-0.2, 5.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.2, pdf["constraint"].max() + 0.2)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["constraint"], 
    ax=ax2, color="lightpink", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"] + 1),  
    ax=ax2, color="lightpink", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["constraint"], 
    ax=ax2, color="turquoise", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"] + 1),  
    ax=ax2, color="turquoise", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='lightpink', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Log of Predicted Productivity vs. Constraint")
ax2.set_xlabel("Constraint")
ax2.set_ylabel("Log(Predicted Productivity)")
ax2.grid(False)

# Set x-axis tick numbers
#ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
#ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax1, color="lightpink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_xticks([])  # Remove x-axis ticks
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

# Right subplot for density distribution of avg_citation_4y (plot third)
ax3 = fig.add_subplot(gs[1, 1])
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 1]["productivity_1416"] + 1), 
    ax=ax3, color="lightpink", alpha=0.5, fill=True, vertical=True
)
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 0]["productivity_1416"] + 1), 
    ax=ax3, color="turquoise", alpha=0.5, fill=True, vertical=True
)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.set_yticks([])  # Remove y-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

# Adjust layout to avoid overlap
plt.tight_layout()

# Save the figure as a high-resolution PNG
plt.savefig("productivity_constraint.png", dpi=300, bbox_inches="tight")

# COMMAND ----------

from PIL import Image
import matplotlib.pyplot as plt

# Load the saved images
img1 = Image.open("productivity_brokerage.png")
img2 = Image.open("productivity_constraint.png")
img3 = Image.open("citation_brokerage.png")
img4 = Image.open("citation_constraint.png")

# Create a 2x2 subplot layout
fig, axes = plt.subplots(2, 2, figsize=(12, 10))

# Titles for subplots
labels = ["(a)", "(b)", "(c)", "(d)"]
images = [img1, img2, img3, img4]

# Display the images and add labels
for ax, img, label in zip(axes.ravel(), images, labels):
    ax.imshow(img)
    ax.axis("off")  # Remove axis labels and ticks
    ax.text(-0.03, 1.0, label, transform=ax.transAxes, fontsize=10, va="top")

fig.text(0.24, 0.11, 
         "Note: 1. The upper limit of the y-axis in each plot is set to one standard deviation above the mean.", 
         ha="center", fontsize=8, va="top")

fig.text(0.161, 0.09, 
         "2. A random sample of 1000 data points was selected.", 
         ha="center", fontsize=8, va="top")

# Adjust layout
plt.tight_layout(rect=[0, 0.05, 1, 1])  # Reserve space for the note

# Save the figure
plt.savefig("2x2_drop1paper.png", dpi=300, bbox_inches="tight")

plt.show()


# COMMAND ----------

from PIL import Image
import matplotlib.pyplot as plt

# Load the saved images
img1 = Image.open("productivity_brokerage.png")
img2 = Image.open("productivity_constraint.png")
img3 = Image.open("citation_brokerage.png")
img4 = Image.open("citation_constraint.png")

# Create a 2x2 subplot layout
fig, axes = plt.subplots(2, 2, figsize=(12, 10))

# Display the images in the subplots
axes[0, 0].imshow(img1)
axes[0, 1].imshow(img2)
axes[1, 0].imshow(img3)
axes[1, 1].imshow(img4)

# Remove axis labels and ticks
for ax in axes.ravel():
    ax.axis("off")

# Adjust layout
plt.tight_layout()

plt.savefig("2x2_drop1paper.png", dpi=300, bbox_inches="tight")


# COMMAND ----------

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.gridspec import GridSpec

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('adjusted_plot.pdf')

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1, 0.1)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig = plt.figure(figsize=(12, 8))
gs = GridSpec(2, 2, width_ratios=[3, 1], height_ratios=[1, 3])  # Define grid layout

# Main subplot for scatter plot and predicted lines (plot first)
ax2 = fig.add_subplot(gs[1, 0])
for women, label, color in [(1, "Women", "lightpink"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = log_predictions_df[log_predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["log_predicted_citation"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        np.log(sampled_subset["avg_citation_4y"] + 1),  # Log-transform citations for consistency
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

# Ensure the y-axis stays within (0,6)
ax2.set_ylim(-0.2, 5.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.2, pdf["normalized_brokerage"].max() + 0.2)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="lightpink", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"] + 1),  
    ax=ax2, color="lightpink", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"] + 1),  
    ax=ax2, color="turquoise", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='lightpink', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Log of Predicted Citation_13 vs. Normalized Brokerage")
ax2.set_xlabel("Normalized Brokerage")
ax2.set_ylabel("Log(Predicted Citation)")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Top subplot for density distribution of normalized_brokerage (plot second)
ax1 = fig.add_subplot(gs[0, 0])
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="lightpink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_xticks([])  # Remove x-axis ticks
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

# Right subplot for density distribution of avg_citation_4y (plot third)
ax3 = fig.add_subplot(gs[1, 1])
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 1]["avg_citation_4y"] + 1), 
    ax=ax3, color="lightpink", alpha=0.5, fill=True, vertical=True
)
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 0]["avg_citation_4y"] + 1), 
    ax=ax3, color="turquoise", alpha=0.5, fill=True, vertical=True
)
ax3.set_xlabel("")  # Remove the "Density" label
ax3.set_xticks([])  # Remove x-axis ticks
ax3.set_yticks([])  # Remove y-axis ticks
ax3.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax3.spines[spine].set_visible(False)

# Adjust layout to avoid overlap
plt.tight_layout()

# Save the current figure to the PDF
plot_file.savefig()

plt.close()  # Close the figure

# Close the PdfPages object
plot_file.close()

# COMMAND ----------

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('adjusted_plot.pdf')

# Step 1: Generate predictions for the log of citation
#normalized_homophily_range = np.linspace(pdf["normalized_gender_homophily"].min(), pdf["normalized_gender_homophily"].max(), 100)
normalized_brokerage_range = np.arange(0, 1, 0.1)
#constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig, (ax1, ax2) = plt.subplots(
    nrows=2, 
    figsize=(10, 8), 
    gridspec_kw={"height_ratios": [1, 3]},  # Adjust height ratios for density and main plot
    sharex=True  # Share x-axis between the two subplots
)

# Plot density distribution on the top subplot (ax1)
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax1, color="lightpink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_xticks([])  # Remove x-axis ticks
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

# Plot marginal effects and scatter points on the bottom subplot (ax2)
for women, label, color in [(1, "Women", "lightpink"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = log_predictions_df[log_predictions_df["women"] == women]
    lines = ax2.plot(
        subset["normalized_brokerage"], 
        subset["log_predicted_citation"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["normalized_brokerage"], 
        np.log(sampled_subset["avg_citation_4y"] + 1),  # Log-transform citations for consistency
        edgecolor='black', color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

# Ensure the y-axis stays within (0,6)
ax2.set_ylim(-0.2, 5.5)

# Ensure the x-axis starts at 0
ax2.set_xlim(-0.2, pdf["normalized_brokerage"].max() + 0.2)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, color="lightpink", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 1]["avg_citation_4y"] + 1),  
    ax=ax2, color="lightpink", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=sampled_pdf[sampled_pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, color="turquoise", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 0]["avg_citation_4y"] + 1),  
    ax=ax2, color="turquoise", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)


# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='lightpink', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Log of Predicted Citation_13 vs. Normalized Brokerage")
ax2.set_xlabel("Normalized Brokerage")
ax2.set_ylabel("Log(Predicted Citation)")
ax2.grid(False)

# Set x-axis tick numbers
ax2.set_xticks([0, 0.2, 0.4, 0.6, 0.8, 1])
ax2.set_xticklabels(["0", "0.2", "0.4", "0.6", "0.8", "1"])

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Remove ticks for the distribution plot
ax1.tick_params(axis='both', which='both', length=0)  # Remove ticks

# Adjust layout to avoid overlap
#plt.tight_layout()

# Save the current figure to the PDF
plot_file.savefig()

plt.close()  # Close the figure

# Close the PdfPages object
plot_file.close()

# COMMAND ----------

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import pandas as pd

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('adjusted_plot.pdf')

# Step 1: Generate predictions for the log of citation
degree_range = np.logspace(np.log10(pdf["degree"].min() + 1), np.log10(pdf["degree"].max() + 1), 100) - 1
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for degree in degree_range:
        data = mean_values.copy()
        data["degree"] = degree
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "degree": degree,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Randomly sample 150 data points
sampled_pdf = pdf.sample(n=150, random_state=42)  # Random sampling with a fixed seed for reproducibility

# Step 3: Plot the figure
fig, (ax1, ax2) = plt.subplots(
    nrows=2, 
    figsize=(10, 8), 
    gridspec_kw={"height_ratios": [1, 3]},  # Adjust height ratios for density and main plot
    sharex=True  # Share x-axis between the two subplots
)

# Plot density distribution on the top subplot (ax1)
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 1]["degree"] + 1), 
    ax=ax1, color="lightpink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=np.log(pdf[pdf["women"] == 0]["degree"] + 1), 
    ax=ax1, color="turquoise", alpha=0.5, fill=True
)
ax1.set_ylabel("")  # Remove the "Density" label
ax1.set_xticks([])  # Remove x-axis ticks
ax1.set_yticks([])  # Remove y-axis ticks
ax1.grid(False)

# Remove the box frame (spines) for the density plot
for spine in ["top", "bottom", "left", "right"]:
    ax1.spines[spine].set_visible(False)

# Plot marginal effects and scatter points on the bottom subplot (ax2)
for women, label, color in [(1, "Women", "lightpink"), (0, "Men", "turquoise")]:
    # Plot the predicted line
    subset = log_predictions_df[log_predictions_df["women"] == women]
    lines = ax2.plot(
        subset["degree"], 
        subset["log_predicted_productivity"], 
        label=label, color=color
    )
    
    # Plot scatter points for the sampled data
    sampled_subset = sampled_pdf[sampled_pdf["women"] == women]
    dots = ax2.scatter(
        sampled_subset["degree"], 
        np.log(sampled_subset["productivity_1416"] + 1),  # Log-transform citations for consistency
        color=color, alpha=0.5, s=50  # Increase dot size to 50
    )

# Apply log scale to x-axis
ax2.set_xscale("symlog", linthresh=10)

# Set x-axis tick positions based on log scale
ax2.set_xticks([1, 10, 100])
ax2.set_xticklabels(["1", "10", "100"])

# Ensure the y-axis stays within a reasonable range
#ax2.set_ylim(-0.2, 5.5)

# Rug plot for Women (lightpink)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 1]["degree"] + 1), 
    ax=ax2, color="lightpink", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 1]["productivity_1416"] + 1),  
    ax=ax2, color="lightpink", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)

# Rug plot for Men (turquoise)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 0]["degree"] + 1), 
    ax=ax2, color="turquoise", height=0.03, alpha=0.6, lw=0.8, axis="x"
)
sns.rugplot(
    data=np.log(sampled_pdf[sampled_pdf["women"] == 0]["productivity_1416"] + 1),  
    ax=ax2, color="turquoise", height=0.05, alpha=0.6, lw=0.8, axis="y", clip_on=True
)


# Create custom legend with a line and a dot
from matplotlib.lines import Line2D
legend_elements = [
    Line2D([0], [0], color='lightpink', marker='o', linestyle='-', markersize=8, label='Women'),
    Line2D([0], [0], color='turquoise', marker='o', linestyle='-', markersize=8, label='Men')
]
ax2.legend(handles=legend_elements)

ax2.set_title("Log of Predicted Productivity vs. Degree (Log Scale)")
ax2.set_xlabel("Degree (Log Scale)")
ax2.set_ylabel("Log(Predicted Productivity)")
ax2.grid(False)

# Remove the top and right box frame from the main plot
for spine in ["top", "right"]:
    ax2.spines[spine].set_visible(False)

# Remove ticks for the distribution plot
ax1.tick_params(axis='both', which='both', length=0)  # Remove ticks

# Adjust layout to avoid overlap
plt.tight_layout()

# Save the current figure to the PDF
plot_file.savefig()
plt.close()  # Close the figure

# Close the PdfPages object
plot_file.close()


# COMMAND ----------

# Group by gender and calculate the required statistics
stats = (
    combined_df
    .groupBy("gender")
    .agg(
        # Average normalized brokerage by gender
        F.avg("normalized_brokerage").alias("avg_normalized_brokerage"),

        # Average brokerage count by gender
        F.avg("brokerage").alias("avg_brokerage"),

        # Percentage of men and women with zero normalized brokerage
        (F.avg(F.when(F.col("normalized_brokerage") == 0, 1).otherwise(0)) * 100).alias("pct_zero_normalized_brokerage"),

        # Average constraint by gender
        F.avg("constraint").alias("avg_constraint")
    )
)

# Show the result
stats.show()


# COMMAND ----------

filtered_focals = combined_df

stats = (
    filtered_focals
    .groupBy("gender")
    .agg(
        # Average normalized brokerage by gender
        F.avg("normalized_brokerage").alias("avg_normalized_brokerage"),

        # Average brokerage count by gender
        F.avg("brokerage").alias("avg_brokerage"),

        # Percentage of men and women with zero normalized brokerage
        (F.avg(F.when(F.col("normalized_brokerage") == 0, 1).otherwise(0)) * 100).alias("pct_zero_normalized_brokerage"),

        # Average constraint by gender
        F.avg("constraint").alias("avg_constraint")
    )
)

# Show the result
stats.show()



# COMMAND ----------

from pyspark.sql import functions as F
import pyspark.pandas as ps

# Define the columns for which we want to calculate statistics
columns_to_aggregate = ['normalized_brokerage', 'brokerage', 'constraint']

# Create the aggregation expressions for each column
agg_columns = []
for col in columns_to_aggregate:
    agg_columns.append(F.avg(F.col(col)).alias(f"avg_{col}"))
    agg_columns.append(F.max(F.col(col)).alias(f"max_{col}"))
    agg_columns.append(F.min(F.col(col)).alias(f"min_{col}"))
    agg_columns.append(F.stddev(F.col(col)).alias(f"stddev_{col}"))
    # Median is calculated using percentile_approx function
    agg_columns.append(F.expr(f"percentile_approx({col}, 0.5)").alias(f"median_{col}"))

# Group by gender and apply all the aggregation expressions
stats = (
    combined_df
    .groupBy("gender")
    .agg(*agg_columns)
)

display(stats)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

df = combined_df
# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Explode countries_focal and subjects_focal to one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.array_contains(F.col("countries_focal"), country).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.array_contains(F.col("subjects_focal"), subject).cast(IntegerType()))

# Step 2: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "productivity_1416", "women", "constraint", "homophily", "degree", "productivity_0913",
    "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 3: Define the regression formula
formula = """
productivity_1416 ~ women + constraint + homophily + degree +
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + years_from_first_pub + interdisciplinary_reach
"""

# Fit the Negative Binomial model
mod = smf.glm(formula=formula, data=pdf, family=sm.families.NegativeBinomial()).fit()

# Step 4: Display Results
results = pd.DataFrame({
    "Variable": mod.params.index,
    "Coefficient": mod.params.values,
    "IRR": np.exp(mod.params.values),  # IRR is exp(coefficient)
    "P-Value": mod.pvalues.values
}).set_index("Variable")

# Add Log-Likelihood as the first row
log_likelihood = mod.llf
results = pd.DataFrame({
    "Coefficient": [log_likelihood],
    "IRR": [None],
    "P-Value": [None]
}, index=["Log-Likelihood"]).append(results)

# Round each result to 4 decimal places
results = results.round(4)
results.to_csv('without_interactions_constraint_0913.csv', index=True)

display(results)


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Explode countries_focal and subjects_focal to one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.array_contains(F.col("countries_focal"), country).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.array_contains(F.col("subjects_focal"), subject).cast(IntegerType()))

# Step 2: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "productivity_1416", "women", "constraint", "homophily", "degree", "productivity_0913",
    "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 3: Define the regression formula with interaction terms
formula = """
productivity_1416 ~ 
women + constraint + homophily + degree +
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + years_from_first_pub + interdisciplinary_reach +
women * constraint + women * homophily + women * degree +
women * country_EU + women * country_Japan + women * country_Canada + women * country_Brazil +
women * subject_MEDI + women * subject_ENGI + women * subject_BUSI_ECON
"""

# Fit the Negative Binomial model
mod = smf.glm(formula=formula, data=pdf, family=sm.families.NegativeBinomial()).fit()

# Step 4: Display Results
results = pd.DataFrame({
    "Variable": mod.params.index,
    "Coefficient": mod.params.values,
    "IRR": np.exp(mod.params.values),  # IRR is exp(coefficient)
    "P-Value": mod.pvalues.values
}).set_index("Variable")

# Add Log-Likelihood as the first row
log_likelihood = mod.llf
results = pd.DataFrame({
    "Coefficient": [log_likelihood],
    "IRR": [None],
    "P-Value": [None]
}, index=["Log-Likelihood"]).append(results)

# Round each result to 4 decimal places
results = results.round(4)

results.to_csv('with_interactions_constraint_0913.csv', index=True)
# Display the results
display(results)


# COMMAND ----------

#negative binomial for predicting 4 year citations

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Explode countries_focal and subjects_focal to one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.array_contains(F.col("countries_focal"), country).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.array_contains(F.col("subjects_focal"), subject).cast(IntegerType()))

# Step 2: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "avg_citation_4y", "women", "normalized_brokerage", "constraint", "normalized_gender_homophily", "degree", "productivity_0913", 
    "avg_year_diff", "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 3: Define the regression formula with interaction terms
formula = """
avg_citation_4y ~ 
women + normalized_brokerage + constraint + normalized_gender_homophily + degree +  
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + avg_year_diff + years_from_first_pub + interdisciplinary_reach +
women * normalized_brokerage + women * constraint + women * normalized_gender_homophily + women * degree 
"""

# Fit the Negative Binomial model
mod = smf.glm(formula=formula, data=pdf, family=sm.families.NegativeBinomial()).fit()

# Step 4: Display Results
results = pd.DataFrame({
    "Variable": mod.params.index,
    "Coefficient": mod.params.values,
    "IRR": np.exp(mod.params.values),  # IRR is exp(coefficient)
    "P-Value": mod.pvalues.values
}).set_index("Variable")

# Add Log-Likelihood as the first row
log_likelihood = mod.llf
results = pd.DataFrame({
    "Coefficient": [log_likelihood],
    "IRR": [None],
    "P-Value": [None]
}, index=["Log-Likelihood"]).append(results)

# Round each result to 4 decimal places
results = results.round(4)

results.to_csv('citation_model_withdrop_greater0nbrokerage_constraint.csv', index=True)
# Display the results
#display(results)


# COMMAND ----------

#get predicted citation under different scenarios

# Calculate mean and standard deviation for normalized_brokerage and constraint
brokerage_mean = pdf["normalized_brokerage"].mean()
brokerage_std = pdf["normalized_brokerage"].std()
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Define the five scenarios
scenarios = [
    {"normalized_brokerage": brokerage_mean + brokerage_std, "constraint": constraint_mean + constraint_std},
    {"normalized_brokerage": brokerage_mean + brokerage_std, "constraint": constraint_mean - constraint_std},
    {"normalized_brokerage": brokerage_mean - brokerage_std, "constraint": constraint_mean + constraint_std},
    {"normalized_brokerage": brokerage_mean - brokerage_std, "constraint": constraint_mean - constraint_std},
    {"normalized_brokerage": brokerage_mean, "constraint": constraint_mean},
]

# Compute predictions for each scenario
predictions = []
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

for scenario in scenarios:
    for women in [1, 0]:  # 1 for women, 0 for men
        data = mean_values.copy()
        data["normalized_brokerage"] = scenario["normalized_brokerage"]
        data["constraint"] = scenario["constraint"]
        data["women"] = women
        predicted_value = mod.predict(pd.DataFrame([data]), linear=False)[0]
        predictions.append({
            "scenario": scenario,
            "women": "Women" if women == 1 else "Men",
            "predicted_citation": predicted_value
        })

# Convert predictions to a DataFrame
predictions_df = pd.DataFrame(predictions)

# Display predictions
display(predictions_df)


# COMMAND ----------

#plot log(citation) vs. normalized brokerage with adjusted constraint 1 SD above mean

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('citation_vs_nbrokerage_with_constraint_adjusted.pdf')

# Step 1: Calculate the mean and standard deviation of 'constraint'
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean + constraint_std

# Step 2: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint  # Set 'constraint' to one standard deviation above the mean
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["log_predicted_citation"], label=label, color=color)

ax1.set_title("Log of Predicted citation vs. Normalized Brokerage\n(Constraint = 1 SD Above Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Log(Predicted citation)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot citation vs. normalized brokerage with constraint adjusted 1 SD above mean

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean + constraint_std

# Generate predictions for citation
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint 
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["predicted_citation"], label=label, color=color)

ax1.set_title("Predicted citation vs. Normalized Brokerage\n(Constraint = 1 SD Above Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Predicted citation")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot log(citation) vs. normalized brokerage with adjusted constraint 1 SD below mean

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Step 1: Calculate the mean and standard deviation of 'constraint'
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean - constraint_std

# Step 2: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint  # Set 'constraint' to one standard deviation above the mean
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["log_predicted_citation"], label=label, color=color)

ax1.set_title("Log of Predicted citation vs. Normalized Brokerage\n(Constraint = 1 SD Below Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Log(Predicted citation)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot citation vs. normalized brokerage with constraint adjusted 1 SD below mean

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean - constraint_std

# Generate predictions for citation
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint 
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["predicted_citation"], label=label, color=color)

ax1.set_title("Predicted citation vs. Normalized Brokerage\n(Constraint = 1 SD Below Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Predicted citation")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()

# COMMAND ----------

#plot log(citation) vs. constraint with adjusted normalized_brokerage 1 SD above mean

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('citation_vs_constraint_with_nbrokerage_adjusted.pdf')

# Step 1: Calculate the mean and standard deviation of 'normalized_brokerage'
normalized_brokerage_mean = pdf["normalized_brokerage"].mean()
normalized_brokerage_std = pdf["normalized_brokerage"].std()

# Adjust 'normalized_brokerage' to one standard deviation above the mean
adjusted_normalized_brokerage = normalized_brokerage_mean + normalized_brokerage_std

# Step 2: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["log_predicted_citation"], label=label, color=color)

ax1.set_title("Log of Predicted Citation vs. Constraint\n(Normalized Brokerage = 1 SD Above Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Log(Predicted Citation)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot citation vs. constraint with adjusted normalized_brokerage 1 SD above mean

# Adjust 'normalized_brokerage' to one standard deviation above the mean
adjusted_normalized_brokerage = normalized_brokerage_mean + normalized_brokerage_std

# Step 2: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["predicted_citation"], label=label, color=color)

ax1.set_title("Predicted Citation vs. Constraint\n(Normalized Brokerage = 1 SD Above Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Predicted Citation")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot log(citation) vs. constraint with adjusted normalized_brokerage 1 SD below mean

# Adjust 'normalized_brokerage' to one standard deviation below the mean
adjusted_normalized_brokerage = normalized_brokerage_mean - normalized_brokerage_std

# Step 2: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["log_predicted_citation"], label=label, color=color)

ax1.set_title("Log of Predicted Citation vs. Constraint\n(Normalized Brokerage = 1 SD Below Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Log(Predicted Citation)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot citation vs. constraint with adjusted normalized_brokerage 1 SD below mean

# Adjust 'normalized_brokerage' to one standard deviation below the mean
adjusted_normalized_brokerage = normalized_brokerage_mean - normalized_brokerage_std

# Step 2: Generate predictions for the log of citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["predicted_citation"], label=label, color=color)

ax1.set_title("Predicted Citation vs. Constraint\n(Normalized Brokerage = 1 SD Below Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Predicted Citation")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()

# COMMAND ----------

#plot log(citation) vs. normalized brokerage and density distribution

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('nbrokerage_vs_citation.pdf')

# Step 1: Generate predictions for the log of citation
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["log_predicted_citation"], label=label, color=color)

ax1.set_title("Log of Predicted Citation vs. Normalized Brokerage")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Log(Predicted Citation)")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot citation vs. normalized brokerage and density distribution

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Step 1: Generate predictions for productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["predicted_citation"], label=label, color=color)

ax1.set_title("Predicted Citation vs. Normalized Brokerage")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Predicted Citation")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()

# COMMAND ----------

#plot log(citation) vs. constraint and density distribution

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('constraint_vs_citation.pdf')

# Step 1: Generate predictions for the log of productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)  
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_citation": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["log_predicted_citation"], label=label, color=color)

ax1.set_title("Log of Predicted Citation vs. Constraint")
ax1.set_xlabel("constraint")
ax1.set_ylabel("Log(Predicted Citation)")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot citation vs. constraint and density distribution

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Step 1: Generate predictions for the citation
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_citation": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["predicted_citation"], label=label, color=color)

ax1.set_title("Predicted Citation vs. Constraint")
ax1.set_xlabel("constraint")
ax1.set_ylabel("Predicted Citation")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()


# COMMAND ----------

#negative binomial

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Explode countries_focal and subjects_focal to one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.array_contains(F.col("countries_focal"), country).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.array_contains(F.col("subjects_focal"), subject).cast(IntegerType()))

# Create a dummy variable for 'normalized_brokerage'
#df = df.withColumn(
#    "dummy_brokerage",
#    F.when(F.col("normalized_brokerage") > 0, 1).otherwise(0).cast(IntegerType())
#)

# Step 2: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "productivity_1416", "women", "normalized_brokerage", "constraint", "normalized_gender_homophily", "degree", "productivity_0913", 
    "years_from_first_pub", "avg_year_diff", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 3: Define the regression formula with interaction terms
formula = """
productivity_1416 ~ 
women + normalized_brokerage + constraint + normalized_gender_homophily + degree +  
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + years_from_first_pub +  avg_year_diff + interdisciplinary_reach +
women * normalized_brokerage + women * constraint + women * normalized_gender_homophily + women * degree 
"""

# Fit the Negative Binomial model
mod = smf.glm(formula=formula, data=pdf, family=sm.families.NegativeBinomial()).fit()

# Step 4: Display Results
results = pd.DataFrame({
    "Variable": mod.params.index,
    "Coefficient": mod.params.values,
    "IRR": np.exp(mod.params.values),  # IRR is exp(coefficient)
    "P-Value": mod.pvalues.values
}).set_index("Variable")

# Add Log-Likelihood as the first row
log_likelihood = mod.llf
results = pd.DataFrame({
    "Coefficient": [log_likelihood],
    "IRR": [None],
    "P-Value": [None]
}, index=["Log-Likelihood"]).append(results)

# Round each result to 4 decimal places
results = results.round(4)

#results.to_csv('nbrokerage_constraint_interactions.csv', index=True)
# Display the results
#display(results)


# COMMAND ----------

#negative binomial with constraint

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType
import statsmodels.api as sm
import statsmodels.formula.api as smf
import pandas as pd
import numpy as np

# Assuming df is already defined as combined_df
df = combined_df

# Step 1: Preprocessing
# Add 'women' column
df = df.withColumn("women", F.when(F.col("gender") == "female", 1).otherwise(0).cast(IntegerType()))

# Explode countries_focal and subjects_focal to one-hot encoding
for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']:
    df = df.withColumn(f"country_{country}", F.array_contains(F.col("countries_focal"), country).cast(IntegerType()))
for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON']:
    df = df.withColumn(f"subject_{subject}", F.array_contains(F.col("subjects_focal"), subject).cast(IntegerType()))

# Step 2: Convert PySpark DataFrame to Pandas for regression
pdf = df.select(
    "productivity_1416", "women", "constraint", "homophily", "degree", "productivity_0913",
    "years_from_first_pub", "interdisciplinary_reach",
    *(f"country_{country}" for country in ['United States', 'EU', 'Japan', 'Canada', 'Brazil']),
    *(f"subject_{subject}" for subject in ['MEDI', 'BIOC', 'ENGI', 'BUSI_ECON'])
).toPandas()

# Drop reference categories for 'countries_focal' and 'subjects_focal'
pdf = pdf.drop(columns=["country_United States", "subject_BIOC"])

# Step 3: Define the regression formula with interaction terms
formula = """
productivity_1416 ~ 
women + constraint + homophily + degree +
country_EU + country_Japan + country_Canada + country_Brazil +
subject_MEDI + subject_ENGI + subject_BUSI_ECON +
productivity_0913 + years_from_first_pub + interdisciplinary_reach +
women * constraint + women * homophily + women * degree
"""

# Fit the Negative Binomial model
mod = smf.glm(formula=formula, data=pdf, family=sm.families.NegativeBinomial()).fit()

# Step 4: Display Results
results = pd.DataFrame({
    "Variable": mod.params.index,
    "Coefficient": mod.params.values,
    "IRR": np.exp(mod.params.values),  # IRR is exp(coefficient)
    "P-Value": mod.pvalues.values
}).set_index("Variable")

# Add Log-Likelihood as the first row
log_likelihood = mod.llf
results = pd.DataFrame({
    "Coefficient": [log_likelihood],
    "IRR": [None],
    "P-Value": [None]
}, index=["Log-Likelihood"]).append(results)

# Round each result to 4 decimal places
results = results.round(4)

#results.to_csv('without_one_paper_interactions_0913.csv', index=True)
# Display the results
#display(results)


# COMMAND ----------

#get predicted productivity under different scenarios

# Calculate mean and standard deviation for normalized_brokerage and constraint
brokerage_mean = pdf["normalized_brokerage"].mean()
brokerage_std = pdf["normalized_brokerage"].std()
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Define the five scenarios
scenarios = [
    {"normalized_brokerage": brokerage_mean + brokerage_std, "constraint": constraint_mean + constraint_std},
    {"normalized_brokerage": brokerage_mean + brokerage_std, "constraint": constraint_mean - constraint_std},
    {"normalized_brokerage": brokerage_mean - brokerage_std, "constraint": constraint_mean + constraint_std},
    {"normalized_brokerage": brokerage_mean - brokerage_std, "constraint": constraint_mean - constraint_std},
    {"normalized_brokerage": brokerage_mean, "constraint": constraint_mean},
]

# Compute predictions for each scenario
predictions = []
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

for scenario in scenarios:
    for women in [1, 0]:  # 1 for women, 0 for men
        data = mean_values.copy()
        data["normalized_brokerage"] = scenario["normalized_brokerage"]
        data["constraint"] = scenario["constraint"]
        data["women"] = women
        predicted_value = mod.predict(pd.DataFrame([data]), linear=False)[0]
        predictions.append({
            "scenario": scenario,
            "women": "Women" if women == 1 else "Men",
            "predicted_productivity": predicted_value
        })

# Convert predictions to a DataFrame
predictions_df = pd.DataFrame(predictions)

# Display predictions
display(predictions_df)


# COMMAND ----------

# get pct of low nbrokerage low constraint and pct of high high

low_low = combined_df.filter(
    (F.col('normalized_brokerage') <= 0.2786) & 
    (F.col('constraint') <= 0.01749)
)
high_high = combined_df.filter(
    (F.col('normalized_brokerage') >= 0.8092) & 
    (F.col('constraint') >= 0.3887)
)
llpct = low_low.count() / combined_df.count()
hhpct = high_high.count() / combined_df.count()

print(llpct)
print(hhpct)

# COMMAND ----------

#plot log(productivity) vs. normalized brokerage with adjusted constraint 1 SD above mean

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('constraint_adjusted.pdf')

# Step 1: Calculate the mean and standard deviation of 'constraint'
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean + constraint_std

# Step 2: Generate predictions for the log of productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint  # Set 'constraint' to one standard deviation above the mean
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["log_predicted_productivity"], label=label, color=color)

ax1.set_title("Log of Predicted Productivity vs. Normalized Brokerage\n(Constraint = 1 SD Above Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Log(Predicted Productivity)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot productivity vs. normalized brokerage with constraint adjusted 1 SD above mean

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean + constraint_std

# Generate predictions for productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint 
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["predicted_productivity"], label=label, color=color)

ax1.set_title("Predicted Productivity vs. Normalized Brokerage\n(Constraint = 1 SD Above Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Predicted Productivity")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot log(productivity) vs. normalized brokerage with adjusted constraint 1 SD below mean

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Step 1: Calculate the mean and standard deviation of 'constraint'
constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean - constraint_std

# Step 2: Generate predictions for the log of productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint  # Set 'constraint' to one standard deviation above the mean
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["log_predicted_productivity"], label=label, color=color)

ax1.set_title("Log of Predicted Productivity vs. Normalized Brokerage\n(Constraint = 1 SD Below Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Log(Predicted Productivity)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot productivity vs. normalized brokerage with constraint adjusted 1 SD below mean

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

constraint_mean = pdf["constraint"].mean()
constraint_std = pdf["constraint"].std()

# Adjust 'constraint' to one standard deviation above the mean
adjusted_constraint = constraint_mean - constraint_std

# Generate predictions for productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        data["constraint"] = adjusted_constraint 
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["predicted_productivity"], label=label, color=color)

ax1.set_title("Predicted Productivity vs. Normalized Brokerage\n(Constraint = 1 SD Below Mean)")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Predicted Productivity")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()

# COMMAND ----------

#plot log(productivity) vs. constraint with adjusted normalized_brokerage 1 SD above mean

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('nbrokerage_adjusted.pdf')

# Step 1: Calculate the mean and standard deviation of 'normalized_brokerage'
normalized_brokerage_mean = pdf["normalized_brokerage"].mean()
normalized_brokerage_std = pdf["normalized_brokerage"].std()

# Adjust 'normalized_brokerage' to one standard deviation above the mean
adjusted_normalized_brokerage = normalized_brokerage_mean + normalized_brokerage_std

# Step 2: Generate predictions for the log of productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["log_predicted_productivity"], label=label, color=color)

ax1.set_title("Log of Predicted Productivity vs. Constraint\n(Normalized Brokerage = 1 SD Above Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Log(Predicted Productivity)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot productivity vs. constraint with adjusted normalized_brokerage 1 SD above mean

# Adjust 'normalized_brokerage' to one standard deviation above the mean
adjusted_normalized_brokerage = normalized_brokerage_mean + normalized_brokerage_std

# Step 2: Generate predictions for the log of productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["predicted_productivity"], label=label, color=color)

ax1.set_title("Predicted Productivity vs. Constraint\n(Normalized Brokerage = 1 SD Above Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Predicted Productivity")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot log(productivity) vs. constraint with adjusted normalized_brokerage 1 SD below mean

# Adjust 'normalized_brokerage' to one standard deviation below the mean
adjusted_normalized_brokerage = normalized_brokerage_mean - normalized_brokerage_std

# Step 2: Generate predictions for the log of productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["log_predicted_productivity"], label=label, color=color)

ax1.set_title("Log of Predicted Productivity vs. Constraint\n(Normalized Brokerage = 1 SD Below Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Log(Predicted Productivity)")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure

# COMMAND ----------

#plot productivity vs. constraint with adjusted normalized_brokerage 1 SD below mean

# Adjust 'normalized_brokerage' to one standard deviation below the mean
adjusted_normalized_brokerage = normalized_brokerage_mean - normalized_brokerage_std

# Step 2: Generate predictions for the log of productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        data["normalized_brokerage"] = adjusted_normalized_brokerage  
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 3: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["predicted_productivity"], label=label, color=color)

ax1.set_title("Predicted Productivity vs. Constraint\n(Normalized Brokerage = 1 SD Below Mean)")
ax1.set_xlabel("Constraint")
ax1.set_ylabel("Predicted Productivity")
ax1.legend()
ax1.grid(False)

# Step 4: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

# Save the plot to the PDF file
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()

# COMMAND ----------

#plot log(productivity) vs. normalized brokerage and density distribution

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('plot_vs_productivity.pdf')

# Step 1: Generate predictions for the log of productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["log_predicted_productivity"], label=label, color=color)

ax1.set_title("Log of Predicted Productivity vs. Normalized Brokerage")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Log(Predicted Productivity)")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot productivity vs. normalized brokerage and density distribution

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Step 1: Generate predictions for productivity
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "normalized_brokerage": normalized_brokerage,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["normalized_brokerage"], subset["predicted_productivity"], label=label, color=color)

ax1.set_title("Predicted Productivity vs. Normalized Brokerage")
ax1.set_xlabel("Normalized Brokerage")
ax1.set_ylabel("Predicted Productivity")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["normalized_brokerage"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["normalized_brokerage"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()

# COMMAND ----------

#plot log(productivity) vs. constraint and density distribution

from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Open a PdfPages object to store all plots in a single PDF
plot_file = PdfPages('constraint_vs_productivity.pdf')

# Step 1: Generate predictions for the log of productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)  
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
log_predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)
        log_predictions.append({
            "constraint": constraint,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })
log_predictions_df = pd.DataFrame(log_predictions)

# Step 2: Plot marginal effects
fig, ax1 = plt.subplots(figsize=(10, 6))

# Marginal effect plot
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["log_predicted_productivity"], label=label, color=color)

ax1.set_title("Log of Predicted Productivity vs. Constraint")
ax1.set_xlabel("constraint")
ax1.set_ylabel("Log(Predicted Productivity)")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot productivity vs. constraint and density distribution

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Step 1: Generate predictions for the productivity
constraint_range = np.linspace(pdf["constraint"].min(), pdf["constraint"].max(), 100)  # Normalized brokerage range
mean_values = pdf.mean().to_dict()  # Mean values for other predictors

# Create predictions DataFrame
predictions = []
for women in [1, 0]:  # 1 for women, 0 for men
    for constraint in constraint_range:
        data = mean_values.copy()
        data["constraint"] = constraint
        data["women"] = women
        pred = mod.predict(pd.DataFrame([data]))
        predictions.append({
            "constraint": constraint,
            "predicted_productivity": pred[0],
            "women": women
        })
predictions_df = pd.DataFrame(predictions)

# Step 2: Plot
fig, ax1 = plt.subplots(figsize=(10, 6))

for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = predictions_df[predictions_df["women"] == women]
    ax1.plot(subset["constraint"], subset["predicted_productivity"], label=label, color=color)

ax1.set_title("Predicted Productivity vs. Constraint")
ax1.set_xlabel("constraint")
ax1.set_ylabel("Predicted Productivity")
ax1.legend()
ax1.grid(False)

# Step 3: Plot distribution on a secondary y-axis
ax2 = ax1.twinx()  # Create a secondary y-axis
sns.kdeplot(
    data=pdf[pdf["women"] == 1]["constraint"], 
    ax=ax2, label="Women Distribution", color="pink", alpha=0.5, fill=True
)
sns.kdeplot(
    data=pdf[pdf["women"] == 0]["constraint"], 
    ax=ax2, label="Men Distribution", color="blue", alpha=0.5, fill=True
)
ax2.set_ylabel("Density")

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()


# COMMAND ----------

#marginal effects of normalized brokerage

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Generate a range of values for normalized_brokerage from 0 to 1 with increment 0.1
normalized_brokerage_range = np.arange(0, 1.1, 0.1)  # Include 1.0 by specifying 1.1 as the upper limit

# Step 2: Fix other variables at their mean
mean_values = pdf.mean()  # Get mean of all variables
mean_values = mean_values.to_dict()

# Create a DataFrame to store predictions and marginal effects
marginal_effects = []

# Step 3: Generate predictions and compute marginal effects for women (1) and men (0)
for women in [1, 0]:  # 1 for women, 0 for men
    for normalized_brokerage in normalized_brokerage_range:
        # Create a data dictionary with mean values and varying normalized_brokerage and women
        data = mean_values.copy()
        data["normalized_brokerage"] = normalized_brokerage
        data["women"] = women

        # Predict log values using the model design matrix and coefficients
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)  # Keep predictions on the log scale
        expected_value = np.exp(log_pred[0])  # Convert log prediction to expected count

        # Marginal effect: beta_1 * expected_value
        marginal_effect = mod.params["normalized_brokerage"] * expected_value
        
        marginal_effects.append({
            "normalized_brokerage": normalized_brokerage,
            "marginal_effect": marginal_effect,
            "women": women,
            "expected_value": expected_value  # Include expected value for reference
        })

# Convert the marginal_effects list to a DataFrame
marginal_effects_df = pd.DataFrame(marginal_effects)

# Step 4: Plot the results
plt.figure(figsize=(10, 6))

# Plot the marginal effects for women and men
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = marginal_effects_df[marginal_effects_df["women"] == women]
    plt.plot(subset["normalized_brokerage"], subset["marginal_effect"], label=label, color=color)

plt.title("Marginal Effect of Normalized Brokerage on Predicted Productivity")
plt.xlabel("Normalized Brokerage")
plt.ylabel("Marginal Effect")
plt.xticks(np.arange(0, 1.1, 0.1))  # Set x-axis ticks to match the range and increment
plt.legend()
plt.show()


# COMMAND ----------

#plot log(productivity) vs. homophily

import matplotlib.pyplot as plt
import numpy as np

# Step 1: Generate a range of values for homophily from 0 to 1 with increment 0.1
homophily_range = np.arange(0, 1.1, 0.1)  # Include 1.0 by specifying 1.1 as the upper limit

# Step 2: Fix other variables at their mean
mean_values = pdf.mean()
mean_values = mean_values.to_dict()

# Create a DataFrame to store predictions
log_predictions = []

# Step 3: Generate predictions for women (1) and men (0), staying on the log scale
for women in [1, 0]:  # 1 for women, 0 for men
    for homophily in homophily_range:
        # Create a data dictionary with mean values and varying homophily and women
        data = mean_values.copy()
        data["homophily"] = homophily
        data["women"] = women

        # Predict log values using the model design matrix and coefficients
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)  # Keep predictions on the log scale
        log_predictions.append({
            "homophily": homophily,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })

# Convert log_predictions to a DataFrame
log_predictions_df = pd.DataFrame(log_predictions)

# Step 4: Plot the results
plt.figure(figsize=(10, 6))
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    plt.plot(subset["homophily"], subset["log_predicted_productivity"], label=label, color=color)

plt.title("Log of Predicted Productivity vs. homophily")
plt.xlabel("homophily")
plt.ylabel("Log(Predicted Productivity)")
plt.xticks(np.arange(0, 1.1, 0.1))  # Set x-axis ticks to match the range and increment
plt.legend()

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure


# COMMAND ----------

#plot productivity vs. interdisciplinary_reach

import matplotlib.pyplot as plt
import numpy as np

# Step 1: Generate a range of values for normalized_brokerage
interdisciplinary_reach_range = np.linspace(pdf["interdisciplinary_reach"].min(), pdf["interdisciplinary_reach"].max(), 100)

# Step 2: Fix other variables at their mean
mean_values = pdf.mean()
mean_values = mean_values.to_dict()

# Create a DataFrame to store predictions
log_predictions = []

# Step 3: Generate predictions for women (1) and men (0), staying on the log scale
for women in [1, 0]:  # 1 for women, 0 for men
    for interdisciplinary_reach in interdisciplinary_reach_range:
        # Create a data dictionary with mean values and varying interdisciplinary_reach and women
        data = mean_values.copy()
        data["interdisciplinary_reach"] = interdisciplinary_reach
        data["women"] = women

        # Predict log values using the model design matrix and coefficients
        log_pred = mod.predict(pd.DataFrame([data]), linear=True)  # Keep predictions on the log scale
        log_predictions.append({
            "interdisciplinary_reach": interdisciplinary_reach,
            "log_predicted_productivity": log_pred[0],
            "women": women
        })

# Convert log_predictions to a DataFrame
log_predictions_df = pd.DataFrame(log_predictions)

# Step 4: Plot the results
plt.figure(figsize=(10, 6))
for women, label, color in [(1, "Women", "pink"), (0, "Men", "blue")]:
    subset = log_predictions_df[log_predictions_df["women"] == women]
    plt.plot(subset["interdisciplinary_reach"], subset["log_predicted_productivity"], label=label, color=color)

plt.title("Log of Predicted Productivity vs. interdisciplinary_reach")
plt.xlabel("interdisciplinary_reach")
plt.ylabel("Log(Predicted Productivity)")
plt.legend()

#plt.show()
plot_file.savefig()  # Save the current figure to the PDF
plt.close()  # Close the figure
plot_file.close()
