from init_values import InitValues
from process.business_logic import  BusinessLogic
from pyspark.sql.functions import *
import datetime

def main():
    # Crea una sesión de Spark
    # Uso de la clase InitValues
    init_values = InitValues()
    spark = init_values.create_spark_session()
    business_logic = BusinessLogic(spark)

    # Convertir la cadena a un objeto de fecha
    fecha_str = "2020-11-21"
    current_date = datetime.datetime.strptime(fecha_str, "%Y-%m-%d").date()
    three_weeks_ago = current_date - datetime.timedelta(days=21)
    one_week_ago = current_date - datetime.timedelta(days=7)

    # Inicializa los dataframes con los datos de entrada
    df_json1, df_json2, df_csv, output_file = init_values.load_dataframes()

    # Expand event_data column in df_json1 and df_json2
    df_last_week_prints, df_last_week_taps, df_3weeks_prints, df_3weeks_taps, df_3weeks_pays = business_logic.filter_by_week(df_json1, df_json2, df_csv, one_week_ago, three_weeks_ago)

    # Mark if there was a tap for each print
    df_prints_with_taps = business_logic.mark_taps(df_last_week_prints, df_last_week_taps)

    # Group dataframes by user_id and value_prop
    df_json1_3weeks_grouped, df_json2_3weeks_grouped, df_csv_3weeks_grouped = business_logic.group_by_user_id_value_prop(df_3weeks_prints, df_3weeks_taps, df_3weeks_pays)

    # Join dataframes to combine the data
    df_final = business_logic.get_final_df(df_prints_with_taps, df_json1_3weeks_grouped, df_json2_3weeks_grouped, df_csv_3weeks_grouped)

    # Show the resulting dataframe
    compresor = "snappy"
    df_final.coalesce(1).write.mode("overwrite").parquet(output_file, compression=compresor)
    
    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
