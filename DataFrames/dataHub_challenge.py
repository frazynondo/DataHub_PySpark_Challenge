import sys
from datetime import datetime
from logging import Logger

from pyspark.sql import *
from pyspark.sql import functions as f
from pyspark.sql.functions import col, lit, when, to_date, udf, rank
from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DateType, FloatType, LongType, \
    DoubleType, DecimalType
from pyspark.sql.functions import sum, avg, max, min, mean, count


# This is used to get trans_fact files ready but with a little modification it can be generic to accommodate many
# different stores with different names that doesnt qualify for a wild card path call

class Job_func(object):
    # def __init__(self):
    #     pass
    @staticmethod
    def Schemas(B):
        trans_keys = ""
        if B == 0:
            trans_keys = "trans_key"
        if B == 1:
            trans_keys = "trans_id"
        Schema = StructType([
            StructField("store_location_key", IntegerType()),
            StructField("product_key", StringType()),
            StructField("collector_key", LongType()),
            StructField("trans_dt", StringType()),
            StructField("sales", DoubleType()),
            StructField("units", IntegerType()),
            # StructField("trans_key", StringType())
            StructField(trans_keys, DecimalType(35, 0))
        ])
        return Schema

    # To combine trans_fact data
    @staticmethod
    def combineData(sparks):
        result1 = sparks.read.csv(path="Data/trans_fact_1.csv", sep=",",
                                  header=True, schema=Job_func.Schemas(0), mode="FAILFAST")
        result1 = Job_func.combine_DF(spark, result1, 2, 11)
        return result1

    @staticmethod
    def combine_DF(sparks, result, left, right):
        for i in range(left, right):
            if i <= 5:
                if i == 4 or i == 5:
                    temp = Job_func.changeSchema(sparks, i)
                else:
                    temp = sparks.read.csv(path="Data/trans_fact_{}.csv".format(i), sep=",",
                                           header=True, schema=Job_func.Schemas(0))
            else:
                temp = Job_func.changeSchema(sparks, i)
            if i == 4 or i == 6 or i == 8:
                uds = udf(lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
                temp = temp.withColumn("trans_dt", uds(col("trans_dt")))
            result = result.unionByName(temp)
            print(result.count(), " Counted")
        return result

    @staticmethod
    def changeSchema(sparks, index):
        try:
            data = sparks.read.csv(path="Data/trans_fact_{}.csv".format(index), sep=",",
                                   header=True, inferSchema=True)
            if index > 5:
                data = data.withColumnRenamed("trans_id", "trans_key")
            data = data.withColumn("store_location_key",
                                   data["store_location_key"]
                                   .cast(IntegerType())).withColumn("collector_key", data["collector_key"]
                                                                    .cast(LongType())) \
                .withColumn("trans_key", data["trans_key"].cast(DecimalType(35, 0))) \
                .withColumn("product_key", data["product_key"].cast(LongType()))
        except IOError as e:
            print("Something is wrong in the change Schema method --> {}".format(e))
        return data

    # Given that we only have one column with nulls we can write this for the purpose of simplicity
    @staticmethod
    def nullsToZero(dataF, colName, newName):
        dataF = dataF.withColumn(colName, when(dataF.collector_key == "-1", "0")
                                 .otherwise(dataF.collector_key).alias(newName)) \
            .withColumnRenamed(colName, newName)
        return dataF

    # Method to perform joins
    @staticmethod
    def joinDataSets(data1, data2, key1: str, key2: str):
        res = data1.join(data2, data1[key1] == data2[key2])
        return res

    @staticmethod
    def Single_CSV_reader(file, sparks):
        try:
            res = sparks.read.csv(path=file, sep=",",
                                  header=True, inferSchema=True)
        except OSError:
            print("Could not open/read file: {}".format(file))
            sys.exit()
        return res


class Results(object):
    @staticmethod
    def ranking(temps, partitions, order_by, limit):
        try:
            window = Window.partitionBy(partitions).orderBy(order_by.desc())
            return temps.select("*", rank().over(window).alias("Rank")).filter(col("Rank") <= limit)
        except IOError as e:
            print("Something is wrong with the passed in parameters: {}".format(e))


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkSQL") \
        .master("local[3]") \
        .getOrCreate()

    # Combine Trans_fact data and Change -1s to 0s values
    df = Job_func.combineData(spark)
    df = Job_func.nullsToZero(df, "collector_key", "COLLECT_KEY")
    df.printSchema()

    # df = df.withColumn("trans_dt", f.to_timestamp(f.col("trans_dt"), "yyyy-dd-mm"))
    # df = df.orderBy("trans_dt")
    # df.show(20)
    # print("Counted total is {}".format(df.count()))

    # Location CSV Data
    file = "Data/location.csv"
    Loc = Job_func.Single_CSV_reader(file, spark)
    Loc = Loc.drop("postal_code").drop("banner").drop("region").drop("store_location_key")
    Loc.show()

    # Product CSV Data
    file = "Data/product.csv"
    Prod = Job_func.Single_CSV_reader(file, spark)
    Prod = Prod.drop("item_description").drop("upc").drop("item_description").drop("sku")

    # Join Location with Combined Trans_fact CSV Data
    Temps = Job_func.joinDataSets(df, Loc, "store_location_key", "store_num")
    Temps = Temps.orderBy("trans_dt")
    Temps = Temps.drop("city")
    Temps.show()

    # Total Sales per province
    province_sales = Temps.groupBy("province").sum("sales").show()

    # Province and it stores' Total
    both_sales = Temps.groupBy("province", "store_num").sum("sales")
    print("PROVINCE AND STORE TOTAL")
    both_sales.orderBy("province").show()

    # Top store vs average store of the Province
    Performance = Temps.groupBy("province").agg(
        sum("sales").alias("Total_sales"),
        max("sales").alias("Province_Top_Store_sales"),
        avg("sales").alias("Average_Store_Sales"),
    ).sort("Province_Top_Store_sales")

    print("TOP STORE VS AVG STORE OF THE PROVINCE")
    Performance.orderBy("province").show(truncate=True)

    # Loyalty program customers vs non - Loyalty program customers
    Prod = Prod.withColumnRenamed("product_key", "product_key1")

    # Join Temps with Products on product key
    Temps = Job_func.joinDataSets(Temps, Prod, "product_key", "product_key1")
    Temps = Temps.drop("product_key1").drop("store_location_key") \
        .withColumn("COLLECT_KEY", Temps["COLLECT_KEY"].cast(LongType()))
    Temps.show(20, truncate=True)

    # Extract Loyalty Customers - those with Collection key greater than 0
    loyalty = Temps.groupBy("province", "COLLECT_KEY", "category").agg(
        sum("sales").alias("Loyalty_sales")).where(col("COLLECT_KEY") > 0)

    # DF for loyalty Customers By Category sales
    C_loyalty = loyalty.groupBy("category").agg(
        sum("Loyalty_sales").alias("Loyalty_sales"))

    # DF with loyal Customers by Province sales
    loyalty = loyalty.groupBy("province").agg(
        sum("Loyalty_sales").alias("Loyalty_sales"))

    # Extract Non-Loyalty Customers - those with Collect_key == 0
    non_loyalty = Temps.groupBy("province", "COLLECT_KEY", "category").agg(
        sum("sales").alias("Non_Loyalty_sales")).where(col("COLLECT_KEY") == 0) \
        .withColumnRenamed("province", "province1")

    # DF with Non_loyal Customer sales by Category
    C_non_loyalty = non_loyalty.groupBy("category").agg(
        sum("Non_Loyalty_sales").alias("Non_Loyalty_sales"))
    non_loyalty.show(truncate=True)

    # DF with Non Loyal Customers By province
    non_loyalty = non_loyalty.groupBy("province1").agg(
        sum("Non_Loyalty_sales").alias("Non_Loyalty_sales"))

    # CREATE A DF FOR Loyal and Non-Loyal category Sales By province
    loyal_non_loyal = Job_func.joinDataSets(loyalty, non_loyalty, "province", "province1")
    loyal_non_loyal = loyal_non_loyal.drop("province1")

    print("LOYAL VS NON LOYAL CUSTOMER SALES BY PROVINCE")
    loyal_non_loyal.show()

    # THE SUM OF LOYAL VS NON-LOYAL COLUMNS
    loyal_non_loyal = loyal_non_loyal.select(
        f.sum(loyal_non_loyal.Loyalty_sales).alias("Loyalty_Total"),
        f.sum(loyal_non_loyal.Non_Loyalty_sales).alias("Non_Loyalty_Total")
    )
    print("THE SUM OF LOYAL VS NON-LOYAL COLUMNS")
    loyal_non_loyal.show(truncate=True)

    # LOYAL VS NON-LOYAL DF BY CATEGORY TOTAL SALES
    C_non_loyalty.sort(col("Non_Loyalty_sales").desc()).show()
    C_loyalty.sort(col("Loyalty_sales").desc()).show()

    # TOP FIVE STORES BY PROVINCE
    top_five = Temps.groupBy("province", "store_num").agg(
        sum("sales").alias("sales")
    )
    top_five.show()
    print("TOP FIVE STORES BY PROVINCE RANKING")
    # window = Window.partitionBy(top_five["province"]).orderBy(top_five["sales"].desc())
    # top_five.select("*", rank().over(window).alias("Rank")).filter(col("Rank") <= 5).show()
    top_five = Results.ranking(top_five, top_five["province"], top_five["sales"], 5)
    top_five.show()

    # Top 10 product categories by department
    top_ten_pro = Temps.groupBy("department", "category") \
        .agg(sum("sales").alias("sales"))

    print("TOP TEN PRODUCTS BY DEPARTMENT")
    # window = Window.partitionBy(top_ten_pro["department"]).orderBy(top_ten_pro["sales"].desc())
    # top_ten_pro.select("*", rank().over(window).alias("Rank")).filter(col("Rank") <= 10).show()
    top_ten_pro = Results.ranking(top_ten_pro, top_ten_pro["department"], top_ten_pro["sales"], 10)
    top_ten_pro.show()
