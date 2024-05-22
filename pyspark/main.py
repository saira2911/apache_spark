from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import configparser


#conf = SparkConf()
#conf.setAppName("nombre aplicacion").setMaster("local[3]")

spark = (SparkSession
        .builder
        #.master("local[3]") configuraciones extra como indicar cual es el master
        #.master("spark://198.289.0.1:7077")
        #.config("master", "local[3]")
        #.config(conf)
        .appName("Example")
        .getOrCreate())



def load_concellos(spark):
    df = (spark.read.format("csv") 
          .option("header", "true")
          .option("inferSchema", "true")
          .load("concellos.csv"))
          
    return df

#df_dup = spark.read.csv("data/concellos.csv", header = True, inferSchema = True)
df = load_concellos(spark)

def rename_col(df):
    df2 = (df
           .withColumnRenamed("C�DIGO POSTAL", "CODIGO_POSTAL")
           .withColumnRenamed("TEL�FONO", "TELEFONO")
           .withColumnRenamed("CORREO ELECTR�NICO", "CORREO_ELECTRONICO")
           .withColumnRenamed("PORTAL WEB", "PORTAL_WEB"))
           
    return df2

df2 = rename_col(df)

def get_domain(columna):
    return f.regexp_replace(
                        f.element_at(
                            f.split(columna, "\."),
                            -1),
                            "/", "")


print(df.columns)

df.show(5)

spark.stop()

