from pyspark.sql import SparkSession
from main import load_concellos, rename_col, get_domain


def test_right_col_loaded():
    spark = (SparkSession
        .builder
        .appName("Example")
        .getOrCreate())

    df = load_concellos(spark)

    assert df.columns == ['CONCELLO', 'PROVINCIA', 'ENDEREZO', 'C�DIGO POSTAL', 'TEL�FONO', 'FAX', 'CORREO ELECTR�NICO', 'PORTAL WEB', 'LATITUD', 'LONGITUD']

def test_col_renamed():
    spark = (SparkSession
        .builder
        .appName("Example")
        .getOrCreate())

    df = load_concellos(spark).transform(rename_col)

    assert df.columns == ['CONCELLO', 'PROVINCIA', 'ENDEREZO', 'CODIGO_POSTAL', 'TELEFONO', 'FAX', 'CORREO_ELECTRONICO', 'PORTAL_WEB', 'LATITUD', 'LONGITUD']

#Siguiendo un proceso iterativo apoyado por los tests

# Requisitos:
# Queremos analizar los dominios de las webs y los mails (.es, .gal, etc)
# Queremos obtener todos los casos en que el dominio de mail y web no coincidan
# Queremos contar los casos en que el dominio de mail y web no coincidan

def test_domain():
    spark = (SparkSession
        .builder
        .appName("Example")
        .getOrCreate())

    df = load_concellos(spark).transform(rename_col)

    (df.withColumn("correo_domain",get_domain("CORREO_ELECTRONICO"))
        .withColumn("web_domain",get_domain("PORTAL_WEB"))
        .where("web_domain != correo_domain")
        .select("CORREO_ELECTRONICO", "PORTAL_WEB", "correo_domain", "web_domain")
        .show(truncate=False))