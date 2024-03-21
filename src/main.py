from init_values import InitValues
from process.business_logic import BusinessLogic
import logging
import datetime

def main():
     # Configure logging
    logging.basicConfig(filename='pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Starting processing')

    try:
        # Create a Spark session
        init_values = InitValues()
        spark = init_values.create_spark_session()
        logging.info('Spark session created correctly')

        business_logic = BusinessLogic(spark)
        
        # Creating date ranges
        current_date = datetime.datetime.now().date()
        three_weeks_ago = current_date - datetime.timedelta(days=21)
        one_week_ago = current_date - datetime.timedelta(days=7)

        # Initialize the dataframes with the input data
        df_json1, df_json2, df_csv, output_file = init_values.load_dataframes()
        logging.info('Dataframes and output path loaded correctly')

        # Preprocessing data for getting sets
        df_last_week_prints, df_last_week_taps, df_3weeks_prints, df_3weeks_taps, df_3weeks_pays = business_logic.filter_by_week(df_json1, df_json2, df_csv, one_week_ago, three_weeks_ago)
        logging.info('Dataframes filtered by ranges')

        # Mark if there was a tap for each print
        df_prints_with_taps = business_logic.mark_taps(df_last_week_prints, df_last_week_taps)
        logging.info('Marked of taps completed')

        # Group dataframes by user_id and value_prop
        df_json1_3weeks_grouped, df_json2_3weeks_grouped, df_csv_3weeks_grouped = business_logic.group_by_user_id_value_prop(df_3weeks_prints, df_3weeks_taps, df_3weeks_pays)
        logging.info('Dataframe grouping completed')

        # Join dataframes to combine the data
        df_final = business_logic.get_final_df(df_prints_with_taps, df_json1_3weeks_grouped, df_json2_3weeks_grouped, df_csv_3weeks_grouped)
        logging.info('Dataframe combination completed')

        # Write the resulting dataframe
        df_final.coalesce(1).write.mode("overwrite").parquet(output_file, compression="snappy")
        logging.info('Dataframe writing completed')

    except Exception as e:
        logging.error(f'Error in the process: {e}')

    finally:
        # Stop the SparkSession
        spark.stop()
        logging.info('Spark session stopped successfully')

if __name__ == "__main__":
    main()

