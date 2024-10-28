# Databricks notebook source

#Bronze Layer

from pyspark.sql import SparkSession
import re

def read_csv(path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    return df

# Load data into DataFrames
country_data = read_csv("dbfs:/FileStore/source_to_bronze/Country_Q1.csv")
department_data = read_csv("dbfs:/FileStore/source_to_bronze/Department_Q1.csv")
employee_data = read_csv("dbfs:/FileStore/source_to_bronze/Employee_Q1.csv")

# COMMAND ----------


# Silver Layer

# Function to convert Camel Case to Snake Case for column names
def camel_to_snake(df):
    new_column_names = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]
    return df.toDF(*new_column_names) 

country_data_snake=camel_to_snake(country_data)
department_data_snake=camel_to_snake(department_data)
employe_data_snake=camel_to_snake(employee_data)

country_data_snake.write.format('parquet').mode("overwrite").save("dbfs:/FileStore/bronze_to_silver")
department_data_snake.write.format('parquet').mode("overwrite").save("dbfs:/FileStore/bronze_to_silver")
employe_data_snake.write.format('parquet').mode("overwrite").save("dbfs:/FileStore/bronze_to_silver")

department_data_snake.display()
employe_data_snake.display()


# COMMAND ----------

#Gold Layer

from pyspark.sql.functions import *

#Finding salary
salary_of_department=employe_data_snake.join(department_data_snake, employe_data_snake.department == department_data_snake.department_i_d,how='inner')

salary=salary_of_department.select(col('employee_name'),col('salary')).orderBy(desc('salary'))

salary.write.format('parquet').mode('overwrite').save("dbfs:/FileStore/silver_to_gold/salary")

#find number of employe in each country

num_of_employes=employe_data_snake.groupBy("country").agg(count("employee_name"))

num_of_employes.write.format('parquet').mode('overwrite').save("dbfs:/FileStore/silver_to_gold/num_of_employes")

# list department name along with country name

dprt_with_country_joins = employe_data_snake.join(department_data_snake , employe_data_snake.department == department_data_snake.department_i_d , how='inner')

selected_dprt_with_country = dprt_with_country_joins.select("department_name","country")

selected_dprt_with_country.write.format('parquet').mode('overwrite').save("dbfs:/FileStore/silver_to_gold/selected_dprt_with_country")

avg_age_employee=employe_data_snake.groupBy("employee_name").agg(avg('age').alias('Average Age'))

avg_age_employee.write.format('parquet').mode('overwrite').save("dbfs:/FileStore/silver_to_gold/avg_age_employee")


# COMMAND ----------


