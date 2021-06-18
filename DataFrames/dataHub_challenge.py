try:
    import io
    from datetime import datetime
    from logging import Logger

    from pyspark.sql import *
    from pyspark.sql import functions as f
    from pyspark.sql.functions import col, lit, when, to_date, udf, rank
    from pyspark.sql.types import StructType, StringType, IntegerType, StructField, DateType, FloatType, LongType, \
        DoubleType, DecimalType

    from google.cloud import storage
    import pandas as pd
    from pyspark.sql.functions import sum, avg, max, min, mean, count
except ImportError as e:
    print("Something wrong with the import {}".format(e))


"""
__CREATE A BUCKET command: gsutil mb -b on -l us-east1 gs://my-awesomefrazy-bucket/
__gcloud init and log into the project of choice
__STEP 1 is to copy all our Data files from e.g local machine to our case to Google Cloud Storage Bucket
gsutil cp DataHomework/product.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/location.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_1.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_2.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_3.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_4.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_5.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_6.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_7.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_8.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_9.csv gs://my-awesomefrazy-bucket
gsutil cp DataHomework/trans_fact_10.csv gs://my-awesomefrazy-bucket


*** Check if the files are succesfully copied by looking at the content of gs://my-awesomefrazy-bucket:
gsutil ls gs://my-awesomefrazy-bucket

*** To avoid incurring charges after we must delete the bucket when done:
gsutil rm -r gs://my-awesomefrazy-bucket

Further documentation on giving someone access/permission to the bucket [1]
[1] https://cloud.google.com/storage/docs/quickstart-gsutil#create
"""
# This is used to get trans_fact files ready but with a little modification it can be generic to accommodate many
# different stores with different names that doesnt qualify for a wild card path call


class Job_func(object):
    def __init__(self):
        pass
        # self.QueResult = []

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
            StructField("trans_key", StringType())
            # StructField(trans_keys, DecimalType(35, 0))
        ])
        return Schema

    # To combine trans_fact data
    @staticmethod
    def combineData(sparks, QueRes):
        # P = ExtractFromGCS()
        # QueResult = P.get_GCS_Data()
        # Converts the string to Pandas DF
        # Note: pandas.read_csv Reads a comma-separated values (csv) file into DataFrame
        pads = pd.read_csv(io.BytesIO(QueRes[0]), encoding="utf-8", sep=",")
        # print(pads.head()) Converts Pandas DF to Spark DataFrame - There is another easier way to extract data from
        # should cloud storage given the proper authentication and installments - If you are not running this locally
        # use this approach instead -->
        # 1. https://cloud.google.com/docs/authentication/getting-started
        # 2. https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark
        # -example#pyspark : The doc is here -> straight way to do this without using Pandas DF. result1 =
        # spark.createDataFrame(pads, schema=Job_func.Schemas(0)) part = "gs://my-awesomefrazy-bucket/trans_fact_1.csv"

        # Note: The createDataFrame method Creates a DataFrame from an RDD, a list or a pandas DataFrame
        result1 = spark.createDataFrame(io.BytesIO(QueRes[0], schema=Job_func.Schemas(0)))
        result1 = Job_func.combine_DF(spark, result1, 4, 10, QueRes)
        return result1

    @staticmethod
    def combine_DF(sparks, result, left, right, QueRes):
        for i in range(left, right):
            # Most files under 4th index are "Trans_key" Column named and above are "Trans_id" column named
            if i <= 4:
                if i == 3 or i == 4:
                    temp = Job_func.changeSchema(sparks, i, QueRes)
                else:
                    pads = pd.read_csv(io.BytesIO(QueRes[i]), encoding="utf-8", sep=",")
                    temp = spark.createDataFrame(pads, schema=Job_func.Schemas(0))
            else:
                temp = Job_func.changeSchema(sparks, i, QueRes)
            if i == 4 or i == 6 or i == 8:
                uds = udf(lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())
                # if when to date toDate, date format - Spark
                temp = temp.withColumn("trans_dt", uds(col("trans_dt")))
            result = result.unionByName(temp)
            print(result.count(), " Counted")
        return result

    @staticmethod
    def changeSchema(sparks, index, QueR):
        # data = sparks.read.csv(path="Data/trans_fact_{}.csv".format(index), sep=",",
        #                        header=True, inferSchema=True)
        data = ExtractFromGCS.String_To_Pd_to_dataFrame(index, QueR)
        if index > 3:
            data = data.withColumnRenamed("trans_id", "trans_key")
        data = data.withColumn("store_location_key",
                               data["store_location_key"]
                               .cast(IntegerType())).withColumn("collector_key", data["collector_key"]
                                                                .cast(LongType())) \
            .withColumn("trans_key", data["trans_key"].cast(DecimalType(35, 0))) \
            .withColumn("product_key", data["product_key"].cast(LongType()))
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


"""
************** NEW CLASS SPACE ****************
"""

"""
Extract the data from GCS Bucket 
"""


class ExtractFromGCS(object):
    def __init__(self):
        self.storage_client = storage.Client.from_service_account_json("Key.json")
        self.BUCKET_NAME = "my-awesomefrazy-bucket"
        self.bucket = self.storage_client.get_bucket(self.BUCKET_NAME)
        self.data = list(self.bucket.list_blobs(prefix=""))

    # Save the string names of trans_fact items from the bucket in a que
    def GCS_Data(self):
        try:
            que = []
            for file in self.data:
                TP = file.name
                if TP[0].lower() == "t":
                    print(TP, "FILE NAME")
                    que.append(TP)
        except IOError as er:
            print("Problems with reading bucket content name".format(er))
        return que

    # Extract location.csv and product.csv name into a que
    def GCS_Data_Non_Trans_fact(self):
        Qu = []
        try:
            for file in self.data:
                # TP = file.name
                if file.name[0].lower() == "l" or file.name[0].lower() == "p":
                    print(file.name, "FILE NAME")
                    Qu.append(file.name)
        except IOError as Ex:
            print("Issues with reading a file: {}".format(Ex))
        return Qu

    # Handle one vs more items in the bucket
    def convertData(self, ques):
        # ques = self.GCS_Data()
        rez = []
        if len(ques) == 0:
            print("No Item in the bucket")
            exit()
        if len(ques) == 1:
            blop = self.bucket.blob(ques.pop(0))
            rez.append(blop.download_as_string())
            return rez
        else:
            while ques:
                T = ques.pop(0)
                blop = self.bucket.blob(T)
                rez.append(blop.download_as_string())
        return rez

    def get_GCS_Data(self, Q):
        return self.convertData(Q)

    @staticmethod
    def String_To_Pd_to_dataFrame(index, QueR):
        # P = ExtractFromGCS()
        # QueR = P.get_GCS_Data()
        pads = pd.read_csv(io.BytesIO(QueR[index]), encoding="utf-8", sep=",")
        print(pads.head())
        reads = spark.createDataFrame(pads)
        return reads


"""
************** NEW CLASS SPACE ****************
"""


class Results(object):
    @staticmethod
    def ranking(temps, partitions, order_by, limit):
        try:
            window = Window.partitionBy(partitions).orderBy(order_by.desc())
            return temps.select("*", rank().over(window).alias("Rank")).filter(col("Rank") <= limit)
        except IOError as e:
            print("Something is wrong with the passed in parameters: {}".format(e))


"""
************** NEW CLASS SPACE ****************
"""

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkSQL") \
        .master("local[3]") \
        .getOrCreate()

    P = ExtractFromGCS()
    Q1 = P.GCS_Data()
    QueResult = P.get_GCS_Data(Q1)
    # Combine Trans_fact data and Change -1 to 0 values
    df = Job_func.combineData(spark, QueResult)
    df = Job_func.nullsToZero(df, "collector_key", "COLLECT_KEY")
    df.printSchema()
    df.show(10)

    # Location CSV Data
    # file = QueResult[10]
    Q2 = P.GCS_Data_Non_Trans_fact()
    QueResult = P.get_GCS_Data(Q2)
    Loc = ExtractFromGCS.String_To_Pd_to_dataFrame(0, QueResult)
    # Loc = Job_func.Single_CSV_reader(file, spark)
    print("LOCATION RESULTS")
    Loc = Loc.drop("postal_code").drop("banner").drop("region").drop("store_location_key")
    Loc.show(10)

    # Product CSV
    print("PRODUCT RESULTS")
    Prod = ExtractFromGCS.String_To_Pd_to_dataFrame(1, QueResult)
    Prod = Prod.drop("item_description")
    Prod.printSchema()
    Prod.show(10)

    # Join Location with Combined Trans_fact CSV Data
    Temps = Job_func.joinDataSets(df, Loc, "store_location_key", "store_num")
    Temps = Temps.withColumn("sales", Temps["sales"].cast(DoubleType())) \
        .withColumn("COLLECT_KEY", Temps["COLLECT_KEY"].cast(LongType()))

    # replace all null values with zero in sales
    Temps = Temps.na.fill(value=0, subset=["sales"])
    Temps = Temps.na.fill(value=0, subset=["units"])
    Temps = Temps.orderBy("trans_dt")
    Temps = Temps.drop("city")
    Temps.show(10)
    Temps.printSchema()

    # Total Sales per province
    province_sales = Temps.groupBy("province").sum("sales").show(10)

    # Province and it stores' Total
    # both_sales = Temps.groupBy("province", "store_num").sum("sales")
    # print("PROVINCE AND STORE TOTAL")
    # both_sales.orderBy("province").show(10)

    # # Top store vs average store of the Province
    Performance = Temps.groupBy("province", "store_location_key").agg(
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

