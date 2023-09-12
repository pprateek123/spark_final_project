# %%
import findspark
findspark.init()
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('solutions_final')\
        .config('spark.driver.extraClassPath','/usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.6.0.jar')\
        .getOrCreate()


# %%
from pyspark.sql.functions import col,row_number, sum ,format_number,desc,year,month,lag,when ,cast,avg
from pyspark.sql.window import Window
import pyspark.sql.functions as f

# %%
#reading the cleaned tables from the postgres database

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
table_name = "cleaned_data"
table_goods = "cleaned_goods"
table_services = "cleaned_services"
table_countries = "cleaned_countries"

df = spark.read \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

# %%
df_goods = spark.read \
    .jdbc(url=jdbc_url, table=table_goods, properties=connection_properties)

df_services = spark.read \
    .jdbc(url=jdbc_url, table=table_services, properties=connection_properties)

df_countries = spark.read \
    .jdbc(url=jdbc_url, table=table_countries, properties=connection_properties)

# %%
df.show()


# %%
df_countries.show()

# %%
df_goods.show()

# %%
df_services.show()

# %%
## top 2 countries with the highest number of transactions 
df_highest = df.groupBy('country_code').agg(sum(col('value')).alias('sum($)'))
df_highest= df_highest.orderBy(desc(col('sum($)')))
df_highest = df_highest.withColumn('sum($)', format_number(col('sum($)'),2)).limit(2)
print("the top 2 countries are : ",df_highest.collect()[0][0],":",df_highest.collect()[0][1],"and" , df_highest.collect()[1][0],':',df_highest.collect()[1][1] )

## only taking the transaction of the top 2 countires (filter first)
df_cn = df.filter(col('country_code')=='CN')
df_au = df.filter(col('country_code')=='AU')

columns = df_au.columns
for i in range(len(columns)):
    df_au = df_au.withColumnRenamed(columns[i], columns[i]+'_au')

## combining both countries transactions horizontally
df_joined = df_cn.join(df_au,df_cn['time_ref']==df_au['time_ref_au'],'inner')

#extracting month and year 
df_joined = df_joined.withColumn("year", year("time_ref")).withColumn("month", month("time_ref"))\
                     .withColumnRenamed('value','value($)')\
                     .withColumnRenamed('value_au','value($)_au')

df_joined.show()
## selecting only the required columns 
df_joined_req_col = df_joined.select('year','month','value($)','value($)_au')



df_joined_req_col = df_joined_req_col.groupBy('year','month')\
                        .agg(sum(col('value($)')).alias('sum($)'),sum(col('value($)_au')).alias('sum($)_au'))
df_joined_req_col.orderBy('year','month').show()

df_joined.printSchema()

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

# df_final.show()difference_au


df_final_formatted = df_final.select('*')


for i in range(len(df_final.columns)-4):
    df_final_formatted = df_final_formatted.withColumn(df_final.columns[i+2],format_number(col(df_final.columns[i+2]),0))

df_final_formatted.show()




# %%
"""
### Question 2 : 
#### TO calculate the most sold(Exported) good to each country in each year. and find how deviated is it from the mean of the good. 

"""

# %%
#####to find the good  sold with the highest txn amount  each country in  2020.###### 

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
# df_joined.show()


## to find out the mean (average value) of each category of goods. 
df_goods_avg = df_each_year.groupBy('code').agg(avg(col('value')).alias('mean_value_goods'))
df_goods_avg = df_goods_avg.withColumn('mean_value_goods', f.round(col('mean_value_goods'),2))
df_goods_unique = df_goods.groupBy('goods_category').agg(f.max('code').alias('code'))
df_joined_avg = df_goods_avg.join(df_goods_unique , on = 'code', how='inner')
# df_joined_avg.show()



## joining the average for each good and previous table

df_joined_final =  df_joined.join(df_joined_avg, on = 'code', how = 'inner')

df_joined_final.orderBy('country_code','year').show()


##percent deviation from mean 

df_deviation = df_joined_final.withColumn('deviation from mean', f.round(col('value')-col('mean_value_goods'),2))

df_deviation.show()

# %%


# %%
