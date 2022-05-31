import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.functions import *


#评论和商品总数
def total_count():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")

    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("asin").createOrReplaceTempView("total_data")
    #overall_data.show()
    #查询临时表对评论等级进行统计
    result = spark.sql("select count(*) as total ,count(distinct(asin)) as pro_total from total_data")
    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())
    #re.columns=['overall','count']
    print(re)
    # 结束
    sc.stop()
    return re

#根据评论等级统计人数
def overall_count():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")

    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("overall").createOrReplaceTempView("overall_data")
    #overall_data.show()
    #查询临时表对评论等级进行统计
    result = spark.sql("select overall as name,count(*) as value from overall_data group by name order by name desc")
    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())
    #re.columns=['overall','count']
    print(re)
    # 结束
    sc.stop()
    return re

#根据商品统计人数
def product_count():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")

    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("asin").createOrReplaceTempView("asin_data")

    #查询临时表对评论等级进行统计并取top5
    result = spark.sql("select asin as name ,count(*) as value from asin_data group by name order by value desc limit 5")
    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())

    print(re)
    # 结束
    sc.stop()
    return re

#根据评论时间统计人数
def date_count():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")
    
    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("unixReviewTime","asin").createOrReplaceTempView("reviewTime_data")

    
    #查询临时表对评论等级进行统计
    result = spark.sql("select distinct(year),sum(cnt) over (partition by year order by year asc) as sum_total  ,sum(pro_cnt) over (partition by year order by year asc) as sum_pro   from (select coalesce(from_unixtime(unixReviewTime,'yyyy'),'other') as year , count(asin) as cnt,  count(distinct asin) as pro_cnt from reviewTime_data group by year,asin order by year desc)")

    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())

    # 结束
    sc.stop()
    return re

#根据helpful统计
def helpful_count():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")
    
    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("helpful","asin").withColumn('helpful_split',regexp_replace(split(col('helpful'), ',')[1], "]", "")).createOrReplaceTempView("helpful_data")
 
    #查询临时表进行统计
    result = spark.sql("""select  asin as name , sum(helpful_split) as value from helpful_data group by name order by value desc limit 5""")
    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())

    # 结束
    sc.stop()
    return re

    
#根据评论字数统计0-100,100-200，200-300
def word_count():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")
    
    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("reviewText").withColumn('word_split', size(split(col('reviewText'), " "))).select("word_split").createOrReplaceTempView("reviewText_data")
 
    #查询临时表对评论等级进行统计
    result = spark.sql("""select count(*) as total,
    count(case when word_split > 0 and word_split <= 100  then 1 end) as a,
    count(case when word_split > 100 and word_split <= 200  then 1 end) as b,
    count(case when word_split > 200 and word_split <= 300  then 1 end) as c,
    count(case when word_split > 300 and word_split <= 400  then 1 end) as d,
    count(case when word_split > 400   then 1 end) as e
    from reviewText_data""")
    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())

    # 结束
    sc.stop()
    return re

#根据评论生成词云
def word_cloud():
    # 创建SparkConf和SparkContext
    conf = SparkConf().setMaster("local").setAppName("wordcount").set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.enabled", "true")

    #读取hdfs上的csv生成dataframe
    df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true' ,delimiter=',').load("hdfs://localhost:9000/test/kindle_review_main3.csv")
    
    #选择dataframe中的某一列数据，然后将数据注册为临时表
    df.select("reviewText").withColumn('split_words', split(col('reviewText'), " ")).withColumn("word_explode",explode(col("split_words"))).groupBy(col("word_explode")).count().withColumnRenamed("count","word_count").orderBy(desc("word_count")).select("word_explode","word_count").createOrReplaceTempView("reviewText_data")


    #查询临时表对评论等级进行统计
    result = spark.sql("""select word_explode as name,word_count as value from reviewText_data limit 50""")
    result.show()

    # 将spark.dataFrame转为pandas.DataFrame
    re = pd.DataFrame(result.toPandas())

    # 结束
    sc.stop()
    return re
    
if __name__ == '__main__':
    #  total_count()
    #overall_count()
    # product_count()
    # date_count()
    # word_count()
    # helpful_count()
    word_cloud()