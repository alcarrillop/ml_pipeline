import os
import configparser
from pyspark.sql import SparkSession

class InitValues:
    def __init__(self):
        self.config_file = os.path.join(os.getcwd(), "config", "config.conf")
        self.config = self.load_config()
        self.spark = self.create_spark_session()

    def load_config(self):
        config = configparser.ConfigParser()
        config.read(self.config_file)
        return config

    def create_spark_session(self):
        spark = SparkSession.builder \
            .master(self.config['Spark']['master']) \
            .appName(self.config['Spark']['app_name']) \
            .getOrCreate()
        return spark

    def load_dataframes(self):
        df_json1 = self.spark.read.json(self.config['Paths']['json1'])
        df_json2 = self.spark.read.json(self.config['Paths']['json2'])
        df_csv = self.spark.read.csv(self.config['Paths']['csv'], header=True)
        output_file = self.config['Paths']['output']
        return df_json1, df_json2, df_csv, output_file
