from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit


class CouncilsJob:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("EnglandCouncilsJob")
                                          .getOrCreate())
        self.input_directory = "/FileStore/data"

    def extract_councils(self):
        df1 = self.spark_session.read.csv(path = self.input_directory +'/england_councils/district_councils.csv' , header=True, inferSchema=True)
        df2 = self.spark_session.read.csv(path = self.input_directory +'/england_councils/london_boroughs.csv' , header=True, inferSchema=True)
        df3 = self.spark_session.read.csv(path = self.input_directory +'/england_councils/metropolitan_districts.csv' , header=True, inferSchema=True)
        df4 = self.spark_session.read.csv(path = self.input_directory +'/england_councils/unitary_authorities.csv' , header=True, inferSchema=True)
        df1 = df1.withColumn("council_type", lit("District Council"))
        df2 = df2.withColumn("council_type", lit("London Borough"))
        df3 = df3.withColumn("council_type", lit("Metropolitan District"))
        df4 = df4.withColumn("council_type", lit("Unitary Authority"))
        dfx1 = df1.union(df2)
        dfx2 = df3.union(df4)
        df = dfx1.union(dfx2)
        return df
    def extract_avg_price(self):
        return self.spark_session.read.csv(path = self.input_directory +'/property_avg_price.csv' , header=True, inferSchema=True)

    def extract_sales_volume(self):
        return self.spark_session.read.csv(path = self.input_directory +'/property_sales_volume.csv' , header=True, inferSchema=True)

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        councils_df.createOrReplaceTempView("council")
        avg_price_df.createOrReplaceTempView("price")
        sales_volume_df.createOrReplaceTempView("sales")
        df = spark.sql("select c.council \
            ,c.county \
            ,c.council_type \
            , p.avg_price_nov_2019 \
            , s.sales_volume_sep_2019 \
            from council c left join price p \
            on c.council = p.local_authority \
            left join sales s ON c.council = s.local_authority \
            --WHERE c.council = 'Durham' \
            ")
        #return display(df.count())
        return df

    def run(self):
        return self.transform(self.extract_councils(), self.extract_avg_price(), self.extract_sales_volume())

if __name__ == "__main__":
    cs = CouncilsJob()
    #cs.extract_councils().show()
    #cs.extract_councils().printSchema()
    #cs.extract_avg_price().show()
    #cs.extract_avg_price().printSchema()
    #cs.extract_sales_volume().show()
    #cs.extract_sales_volume().printSchema()
    cs.run().show()