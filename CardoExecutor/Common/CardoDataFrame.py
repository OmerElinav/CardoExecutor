from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
from CardoExecutor.Common.CardoWrapper import CardoWrapper, get_wrapped_attribute

_DF = '_df'
_RDD = '_rdd'


class CardoDataFrame(DataFrame, metaclass=CardoWrapper):
    def __init__(self, df: DataFrame):
        self._df = df

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item, _DF)

    def to_cardo_pandas(self):
        return CardoPandasDataFrame(self._df.toPandas())

    def to_cardo_rdd(self):
        return CardoRDD(self._df.rdd)


class CardoPandasDataFrame(pd.DataFrame, metaclass=CardoWrapper):
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item, _DF)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._df))


class CardoRDD(RDD, metaclass=CardoWrapper):
    def __init__(self, rdd: RDD):
        self._rdd = rdd

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item, _RDD)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._df))
