# %%
#importing necessary libraries and starting spark session
from os import environ
import yaml
import findspark
findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('solutions_final')\
        .config('spark.driver.extraClassPath','/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .config('spark.jars','/opt/spark/jars/postgresql-42.6.0.jar')\
        .getOrCreate()

# Define the path to your YAML file
yaml_file_path = 'config.yaml'

# Read the YAML file and parse it into a Python dictionary
with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)




# %%
#importing necessary funcitons from spark
from pyspark.sql.functions import col,row_number, sum ,format_number,desc,year,month,lag,when ,cast,avg,substring,lit, length,expr,concat_ws, to_date,udf
from pyspark.sql.window import Window
import pyspark.sql.functions as f



# %%
pg_user = environ.get("DB_USER")
pg_password = environ.get("DB_PASSWORD")

# %% [markdown]
# ## CLEANING PART
# 

# %%
#reading csv from local folder

data_path = "./data/revised_final.csv"  
df = spark.read.csv(data_path, header=True, inferSchema=True)
#loading goods_classifiation 
data_path = "./data/goods_classification.csv"  
df_goods = spark.read.csv(data_path, header=True, inferSchema=True)

data_path = "./data/country_classification.csv"  
df_country = spark.read.csv(data_path, header=True, inferSchema=True)
data_path = "./data/services_classification.csv"  
df_services = spark.read.csv(data_path, header=True, inferSchema=True)


# %%
##exploring the main df
df.show()
df.printSchema()
df.count()

# %%
#changing time_ref column to string appeding month and changing it into date format 

df = df.withColumn("time_ref", col("time_ref").cast("string"))

df = df.withColumn("time_ref", concat_ws("/", substring(col("time_ref"), 1, 4), substring(col("time_ref"), 5, 2), lit('30')))
df = df.withColumn("time_ref", to_date(col("time_ref"), "yyyy/MM/dd"))



# %%

## filtering unnecessary data and  changing the length of goods column for ease in join

df = df.filter(df['country_code'] != 'TOT')

df = df.filter(df['country_code'] != 'TOT (BoP basis)')

df = df.filter(df['country_code'] != 'TOT (OMT FOB)')
df = df.filter(df['country_code'] != 'TOT (OMT CIF)')
df = df.filter(df['country_code'] != 'TOT (OMT VFD)')



df = df.withColumn("code",
    when((col("product_type") == "Goods") & (length(col("code")) == 4), substring(col("code"), 1, 2))
    .when((col("product_type") == "Goods") & (length(col("code")) == 3), substring(col("code"), 1, 1))
    .otherwise(col("code")))

# %%
## renaming column from df_goods for ease 

df_goods = df_goods.withColumnRenamed('Level_1','code')\
        .withColumnRenamed('Level_1_desc','goods_category')

# %%
#writing file in parquet format in local  
df.write.format("parquet")\
    .mode('overwrite')\
    .options(compression="snappy")\
    .save("./data_cleaned/revised_final_cleaned/cleaned_revised.parquet")



df_goods.coalesce(1).write.format("parquet").mode("overwrite").\
options(compression='snappy').\
save("./data_cleaned/goods_cleaned/cleaned_goods.parquet")
df_services.coalesce(1).write.format("parquet").mode("overwrite").\
options(compression='snappy').\
save("./data_cleaned/services_cleaned/cleaned_sevices.parquet")
df_country.coalesce(1).write.format("parquet").mode("overwrite").\
options(compression='snappy').\
save("./data_cleaned/countries_cleaned/cleaned_countries.parquet")


# %%
#writing files to the prostgres table 
df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'cleaned_data', user=config['postgres']["user"],password=config['postgres']["password"] ).mode('overwrite')
df_goods.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'cleaned_goods', user=config['postgres']["user"],password=pg_password ).mode('overwrite')
df_services.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'cleaned_services', user=config['postgres']["user"],password=pg_password ).mode('overwrite')
df_country.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'cleaned_countries', user=config['postgres']["user"],password=pg_password ).mode('overwrite')

# %% [markdown]
# ## SOLUTIONS PART 

# %%
#reading the cleaned tables from the postgres database

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}
table_name = "cleaned_data"
table_goods = "cleaned_goods"
table_services = "cleaned_services"
table_countries = "cleaned_countries"


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "cleaned_data") \
    .option("user", config['postgres']["user"]
) \
    .option("password", config['postgres']["user"]
).load()

df_goods = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "cleaned_goods") \
    .option("user", config['postgres']["user"]) \
    .option("password", config['postgres']["user"]).load() 

df_services = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "cleaned_services") \
    .option("user", config['postgres']["user"]) \
    .option("password", config['postgres']["password"]).load()

df_countries = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "cleaned_countries") \
    .option("user", config['postgres']["user"]) \
    .option("password", config['postgres']["password"]).load()


# df = spark.read \
#     .jdbc(url=jdbc_url, table=table_name, properties=connection_properties).load()
# df_goods = spark.read \
#     .jdbc(url=jdbc_url, table=table_goods, properties=connection_properties).load()

# df_services = spark.read \
#     .jdbc(url=jdbc_url, table=table_services, properties=connection_properties).load()

# df_countries = spark.read \
#     .jdbc(url=jdbc_url, table=table_countries, properties=connection_properties).load()

# %% [markdown]
# ### Question 1:
# #### Trend analysis of the two countries which has the highest transactIons with new zealand. Increasing and decreasing trends of the sum of the transaction in each quarter month.



## top 2 countries with the highest number of transactions 
df_highest = df.groupBy('country_code').agg(sum(col('value')).alias('sum($)'))
df_highest= df_highest.orderBy(desc(col('sum($)')))
df_highest = df_highest.withColumn('sum($)', format_number(col('sum($)'),2)).limit(2)
print("the top 2 countries are : ",df_highest.collect()[0][0],":",df_highest.collect()[0][1],"and" , df_highest.collect()[1][0],':',df_highest.collect()[1][1] )

## only taking the transaction of the top 2 countires (filter first)
df_cn = df.filter(col('country_code')=='CN')
df_au = df.filter(col('country_code')=='AU')

##renaming columns
columns = df_au.columns
for i in range(len(columns)):
    df_au = df_au.withColumnRenamed(columns[i], columns[i]+'_au')

## combining both countries transactions horizontally
df_joined = df_cn.join(df_au,df_cn['time_ref']==df_au['time_ref_au'],'inner')

#extracting month and year 
df_joined = df_joined.withColumn("year", year("time_ref")).withColumn("month", month("time_ref"))\
                     .withColumnRenamed('value','value($)')\
                     .withColumnRenamed('value_au','value($)_au')


## selecting only the required columns 
df_joined_req_col = df_joined.select('year','month','value($)','value($)_au')



df_joined_req_col = df_joined_req_col.groupBy('year','month')\
                        .agg(sum(col('value($)')).alias('sum($)'),sum(col('value($)_au')).alias('sum($)_au'))


#window
window_spec = Window.orderBy("year","month")

df_joined_req_col = df_joined_req_col.withColumn('lag($)',lag(col('sum($)'),1).over(window_spec))\
                                     .withColumn('lag_au($)',lag(col('sum($)_au'),1).over(window_spec))
df_joined_req_col = df_joined_req_col.na.drop()

#difference and trend analysis
df_final = df_joined_req_col.withColumn('difference($)',col('sum($)')-col('lag($)'))\
                            .withColumn('difference_au($)',col('sum($)_au')-col('lag_au($)'))


df_final = df_final.withColumn('change', when(col('difference($)')>0,"Increase").otherwise('Decrease'))\
                    .withColumn('change_au',when(col('difference_au($)')>0,'Increase').otherwise('Decrease'))


df_final_formatted = df_final.select('*')

## to show in human readable form
for i in range(len(df_final.columns)-4):
    df_final_formatted = df_final_formatted.withColumn(df_final.columns[i+2],format_number(col(df_final.columns[i+2]),0))

df_final_formatted.show()




# %%
#writing the table to postgres 
df_final.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'task1', user=pg_user,password=pg_password).mode('overwrite')


# %% [markdown]
# ### Question 2 : 
# #### TO calculate the most sold(Exported) good to each country in each year from NewZealand and  Find how deviated is it from the mean transaction  of that particular good. 
# 

# %%

#finding just the sold goods
df_sold = df.filter(col('account')=='Exports')
df_sold = df_sold.filter(col('product_type')=='Goods')

#extracting year
df_each_year = df_sold.withColumn('year', year('time_ref'))


## finding out which country has imported max sum of  good in a year
df_result =df_each_year.groupBy('country_code','year').agg(f.max(col('value')).alias('value'))

#joining the main table with teh grouped one to find out which country imported how much in a particular year along with other details
df_joined = df_each_year.join(df_result , on = ['country_code','value','year'] , how = 'inner')
df_joined  = df_joined.dropDuplicates()
df_joined = df_joined.filter(col('code')!=00)
df_joined = df_joined.orderBy('country_code','year')


## to find out the mean (average value) of each category of goods. 
df_goods_avg = df_each_year.groupBy('code').agg(avg(col('value')).alias('mean_value_goods'))
df_goods_avg = df_goods_avg.withColumn('mean_value_goods', f.round(col('mean_value_goods'),2))

df_goods_unique = df_goods.groupBy('goods_category').agg(f.max('code').alias('code'))
df_joined_avg = df_goods_avg.join(df_goods_unique , on = 'code', how='inner')



## joining the average for each good and previous table

df_joined_final =  df_joined.join(df_joined_avg, on = 'code', how = 'inner')



##percent deviation from mean 

df_deviation = df_joined_final.withColumn('deviation from mean', f.round(col('value')-col('mean_value_goods'),2))

df_deviation.select('year','country_code','value','mean_value_goods','goods_category','deviation from mean').orderBy('year').show()

# %%
##writing thi dataframe to postgres
df_deviation.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres',driver = 'org.postgresql.Driver', dbtable = 'task2', user=pg_user,password=pg_password).mode('overwrite')


# %%
#stopping the spark session
spark.stop()


