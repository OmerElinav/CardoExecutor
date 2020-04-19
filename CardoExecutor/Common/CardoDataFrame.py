from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
from CardoExecutor.Common.CardoWrapper import get_wrapped_attribute, _INNER


class CardoDataFrame(DataFrame):
    def __init__(self, df: DataFrame, name: str = ""):
        self._inner = df
        self.name = name

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item)

    def to_cardo_pandas(self):
        return CardoPandasDataFrame(self._inner.toPandas(), name=self.name)

    def to_cardo_rdd(self):
        return CardoRDD(self._inner.rdd, name=self.name)


class CardoPandasDataFrame(pd.DataFrame):
    def __init__(self, df: pd.DataFrame, name: str = ""):
        object.__setattr__(self, _INNER, df)
        object.__setattr__(self, "name", name)

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._inner), name=self.name)


class CardoRDD(RDD):
    def __init__(self, rdd: RDD, name: str = ""):
        self._inner = rdd
        self.name = name

    def __getattribute__(self, item):
        return get_wrapped_attribute(self, item)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._inner), name=self.name)
