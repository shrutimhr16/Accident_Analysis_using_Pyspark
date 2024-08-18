import pyspark
from pyspark.sql import SparkSession
import yaml

def get_spark(appname):
    spark=SparkSession.builder.appName(appname).getOrCreate()
    return spark

def read_yaml(path):
    """
    Read Config file in YAML format
    :param file_path: file path to config.yaml
    :return: dictionary with config details
    """
    with open(path, 'r') as file:
        config = yaml.safe_load(file)
        print(config)
        return config

def read_csv(spark,file_path):
    """
    Read csv file and convert it to dataframe
    :param spark:spark instance
    :param file_path: file path of input csv
    :return: dataframe
    """
    return spark.read.option("inferSchema",'true').csv(file_path,header=True)

def write_output(df , output_path , output_format):
    """
    Read csv file and convert it to dataframe
    :param spark:spark instance
    :param file_path: file path of input csv
    :return: dataframe
    """
    df.repartition(1).write.format(output_format).mode('overwrite').option('header',"true").save(output_path)
