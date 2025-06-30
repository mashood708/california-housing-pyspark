# california-housing-pyspark
This project analyzes California housing data using PySpark running in Google Colab. It demonstrates scalable data engineering and exploratory data analysis (EDA) in a cloud-based Jupyter notebook environment.
The notebook processes a housing dataset with features such as:

Median house value

Median income

Housing median age

Population

Geographical coordinates (longitude & latitude)

and performs transformations, aggregations, and correlation analysis to extract meaningful insights.

🚀 Technologies Used
PySpark (Apache Spark)

Google Colab (Jupyter-based environment)

Python 3

📊 Features
✅ Load CSV data from Colab’s file upload
✅ Cast columns for numeric correlation
✅ Group-by analysis on median income brackets
✅ Generate summary statistics
✅ Correlation analysis between median income and house value
✅ Exploratory queries and data cleaning

📂 Files
california_housing_pyspark_analysis.ipynb — main Colab notebook

housing.csv — sample data file (upload manually to Colab session)

📈 How to Run on Google Colab
Open the notebook in Google Colab

Upload your housing.csv file via the Colab file upload panel

Modify the file path in the code if needed (/content/housing.csv)

Run each cell in sequence

📝 Sample Code
python
Copy
Edit
# Install PySpark in Colab
!pip install pyspark

# Start a Spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("CaliforniaHousing").getOrCreate()

# Load data
df = spark.read.option("header", True).option("inferSchema", True).csv("/content/housing.csv")

# Quick summary
df.show(5)

# Group by median income
from pyspark.sql import functions as F
df.groupBy(
    F.round("median_income", 1).alias("income_bracket")
).agg(
    F.avg("median_house_value").alias("avg_house_value")
).orderBy("income_bracket").show()
