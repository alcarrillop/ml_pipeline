from pyspark.sql import SparkSession

def create_spark_session():
    """
    Crea una sesión de Spark y devuelve el objeto SparkSession.
    """
    spark = SparkSession.builder \
        .appName("Inicialización de datos") \
        .getOrCreate()
    return spark

def init_dataframes(spark, input_directory):
    """
    Inicializa los dataframes con los datos de entrada y devuelve los dataframes.
    """
    # Ejemplo de carga de datos desde archivos CSV y JSON
    df_csv = spark.read.csv(input_directory + "/pays.csv", header=True, inferSchema=True)
    df_json1 = spark.read.json(input_directory + "/prints.json")
    df_json2 = spark.read.json(input_directory + "/taps.json")

    return df_csv, df_json1, df_json2
