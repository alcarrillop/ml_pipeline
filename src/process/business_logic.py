from pyspark.sql.functions import *
class BusinessLogic:
    def __init__(self, spark):
        self.spark = spark

    def filter_by_week(self, df_json1, df_json2, df_csv, one_week_ago, three_weeks_ago):
        """Filter dataframes to only include data from the past week and three weeks"""
        df_prints = df_json1.withColumn("day", col("day").cast("date")).selectExpr("user_id", "day", "event_data.value_prop as value_prop")
        df_taps = df_json2.withColumn("day", col("day").cast("date")).selectExpr("user_id", "day", "event_data.value_prop as value_prop")
        df_pays = df_csv.withColumn("pay_date", col("pay_date").cast("date"))

        df_last_week_prints = df_prints.filter(col("day") > one_week_ago)
        df_last_week_taps = df_taps.filter(col("day") > one_week_ago)

        df_3weeks_prints = df_prints.filter((col("day") > three_weeks_ago) & (col("day") < one_week_ago))
        df_3weeks_taps = df_taps.filter((col("day") > three_weeks_ago) & (col("day") < one_week_ago))
        df_3weeks_pays = df_pays.filter((col("pay_date") > three_weeks_ago) & (col("pay_date") < one_week_ago))
        return df_last_week_prints, df_last_week_taps, df_3weeks_prints, df_3weeks_taps, df_3weeks_pays

    def mark_taps(self, df_last_week_prints, df_last_week_taps):
        """Mark if there was a tap for each print"""
        df_prints_with_taps = df_last_week_prints.alias("prints")\
                    .join(df_last_week_taps.alias("taps"), 
                        on = ["user_id", "value_prop", "day"], 
                        how = "left_outer")\
                    .select("prints.*", when(col("taps.day").isNull(), False).otherwise(True).alias("clicked"))
        return df_prints_with_taps

    def group_by_user_id_value_prop(self, df_3weeks_prints, df_3weeks_taps, df_3weeks_pays):
        """Group dataframe by user_id and value_prop"""
        df_json1_3weeks_grouped = df_3weeks_prints.groupBy("user_id", "value_prop").count().withColumnRenamed("count", "print_count_3weeks")
        df_json2_3weeks_grouped = df_3weeks_taps.groupBy("user_id", "value_prop").count().withColumnRenamed("count", "tap_count_3weeks")
        df_csv_3weeks_grouped = df_3weeks_pays.groupBy("user_id", "value_prop")\
            .agg(count("total").alias("payment_count_3weeks"), sum("total").alias("total_spent_3weeks"))

        return df_json1_3weeks_grouped, df_json2_3weeks_grouped, df_csv_3weeks_grouped

    def get_final_df(self, df_prints_with_taps, df_json1_3weeks_grouped, df_json2_3weeks_grouped, df_csv_3weeks_grouped):
        """Join dataframes to combine the data"""
        df_final = df_prints_with_taps.join(df_json1_3weeks_grouped, ["user_id", "value_prop"], "left")\
            .join(df_json2_3weeks_grouped, ["user_id", "value_prop"], "left")\
            .join(df_csv_3weeks_grouped, ["user_id", "value_prop"], "left") 
        return df_final