# **Machine Learning Model Pipeline for Value Proposition Prediction**

## **Description**

This project aims to develop a Machine Learning model to predict the order of value propositions (value props) in the carousel app named "Descubrí Más" within the Mercado Pagos business unit. The task involves building a pipeline using Python and its libraries to process data from three different sources and prepare a dataset for model ingestion.

## **Data Sources**

The pipeline utilizes the following data sources:

- **Prints (prints.json):** Historical data of displayed value props to users in JSON lines format.
- **Taps (taps.json):** Historical data of clicked value props by users, also in JSON lines format.
- **Payments (pays.csv):** Historical data of payments made by users, stored in CSV format.

## **Expected Results**

The expected output includes the following information:

- Prints from the last week.
- For each print:
    - A field indicating if the value props were clicked or not.
    - Number of views for each value prop in the last 3 weeks prior to the print.
    - Number of times a user clicked on each value prop in the last 3 weeks prior to the print.
    - Number of payments made by the user for each value prop in the last 3 weeks prior to the print.
    - Accumulated payments made by the user for each value prop in the last 3 weeks prior to the print.

## **Proposed Process**

1. **Create a Spark Session:** Initialize a Spark session for data processing.
2. **Load Data:** Load input dataframes from the specified sources.
3. **Preprocessing:** Filter dataframes to get sets for the last week and the last 3 weeks.
4. **Mark Taps:** Identify if there was a tap for each print.
5. **Group Dataframes:** Group dataframes by user_id and value_prop.
6. **Combine Dataframes:** Join dataframes to combine the relevant data.
7. **Write Dataframe:** Write the resulting dataframe to a parquet file with Snappy compression.

## Sample table with initial data
| user_id | value_prop         | day        | clicked | print_count_3weeks | tap_count_3weeks | payment_count_3weeks | total_spent_3weeks |
|---------|--------------------|------------|---------|--------------------|------------------|----------------------|--------------------|
| 4335    | credits_consumer   | 2020-11-17 | false   | 1                  | NULL             | 1                    | 23.5               |
| 14003   | point              | 2020-11-29 | true    | NULL               | NULL             | NULL                 | NULL               |
| 24793   | credits_consumer   | 2020-11-29 | false   | 2                  | 1                | NULL                 | NULL               |
| 24793   | link_cobro         | 2020-11-29 | false   | 1                  | NULL             | NULL                 | NULL               |
| 27016   | transport          | 2020-11-17 | false   | NULL               | NULL             | 1                    | 21.22              |
| 48531   | send_money         | 2020-11-15 | false   | 2                  | NULL             | 1                    | 18.31              |
| 68846   | transport          | 2020-11-21 | true    | NULL               | NULL             | 1                    | 6.13               |
| 71567   | send_money         | 2020-11-21 | false   | NULL               | NULL             | NULL                 | NULL               |
| 82357   | credits_consumer   | 2020-11-21 | true    | NULL               | NULL             | 1                    | 24.25              |
| 85957   | prepaid            | 2020-11-25 | false   | NULL               | NULL             | NULL                 | NULL               |
| 86246   | cellphone_recharge | 2020-11-29 | false   | 1                  | NULL             | 1                    | 188.73             |
| 88073   | transport          | 2020-11-29 | false   | NULL               | NULL             | 1                    | 58.55              |

## Code Snippet

```python
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

```
