from init_values import create_spark_session, init_dataframes
from pyspark.sql.functions import *
import datetime

def main():
    # Crea una sesión de Spark
    spark = create_spark_session()

    # Directorio de entrada
    input_directory = "./input"
    output_directory = "./output"

    # Inicializa los dataframes con los datos de entrada
    df_csv, df_json1, df_json2 = init_dataframes(spark, input_directory)

    # Imprime el esquema de cada dataframe
    print("Esquema del dataframe CSV:")
    df_csv.printSchema()

    print("Esquema del dataframe JSON 1:")
    df_json1.printSchema()

    print("Esquema del dataframe JSON 2:")
    df_json2.printSchema()

    # Esto es una prueba

if __name__ == "__main__":
    main()
