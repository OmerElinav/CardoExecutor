from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
from CardoExecutor.Common.CardoWrapper import CardoWrapper, get_wrapped_attribute


class CardoDataFrame(DataFrame, metaclass=CardoWrapper):
    def __init__(self, df: DataFrame):
        self._inner = df

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item)

    def to_cardo_pandas(self):
        return CardoPandasDataFrame(self._df.toPandas())

    def to_cardo_rdd(self):
        return CardoRDD(self._inner.rdd)


class CardoPandasDataFrame(pd.DataFrame, metaclass=CardoWrapper):
    def __init__(self, df: pd.DataFrame):
        self._inner = df

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._inner))


class CardoRDD(RDD, metaclass=CardoWrapper):
    def __init__(self, rdd: RDD):
        self._inner = rdd

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._inner))
