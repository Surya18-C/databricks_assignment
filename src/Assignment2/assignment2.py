
import requests


response = requests.get('https://reqres.in/api/users?page=2')
user_data = response.json()


user_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("avatar", StringType(), True)
])

# Define schema for the entire API response
response_schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("per_page", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("total_pages", IntegerType(), True),
    StructField("data", ArrayType(user_schema), True),
    StructField("support", MapType(StringType(), StringType()), True)
])


user_df = spark.createDataFrame([user_data], response_schema)


user_df_cleaned = user_df.drop('page', 'per_page', 'total', 'total_pages', 'support')


exploded_df = user_df_cleaned.withColumn('data', explode('data'))


final_df = exploded_df.select(
    exploded_df.data.id.alias("user_id"),
    exploded_df.data.email.alias("user_email"),
    exploded_df.data.first_name.alias("first_name"),
    exploded_df.data.last_name.alias("last_name"),
    exploded_df.data.avatar.alias("avatar_url")
)


final_df_with_site = final_df.withColumn("site_address", split(final_df["user_email"], "@")[1])


final_data = final_df_with_site.withColumn('load_date', current_date())

final_data.write.format('delta').mode('overwrite').save('dbfs:/FileStore/assignments/question2/site_info/user_info')


loaded_data = spark.read.format('delta').load('dbfs:/FileStore/assignments/question2/site_info/user_info')

display(loaded_data)
