from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession
import pandas as pd

_DF = '_df'
_RDD = '_rdd'

def _wrap(func, wrapping_class):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        return wrapping_class(result) if \
            isinstance(result, wrapping_class.__bases__[0]) \
            else result

    return wrapper


def _wrap_bases(wrapping_class):
    for base in wrapping_class.__bases__:
        for key, value in vars(base).items():
            if callable(value):
                wrap = _wrap(getattr(base, key), wrapping_class)
                setattr(wrapping_class, key, wrap)
        return base


class CardoWrapper(type):
    def __init__(cls, name, bases, namespace):
        super(CardoWrapper, cls).__init__(name, bases, namespace)


def _get_attribute(cardo_dataframe, item, df_name=_DF):
    df = object.__getattribute__(cardo_dataframe, df_name)
    df_type = type(df)
    if hasattr(df_type, item):
        return df_type.__getattribute__(df, item)
    return df_type.__getattribute__(cardo_dataframe, item)


class CardoDataFrame(DataFrame, metaclass=CardoWrapper):
    def __init__(self, df: DataFrame):
        self._df = df

    def __getattribute__(self, item):
        return _get_attribute(self, item)

    def to_cardo_pandas(self):
        return CardoPandasDataFrame(self._df.toPandas())

    def to_cardo_rdd(self):
        return CardoRDD(self._df.rdd)


class CardoPandasDataFrame(pd.DataFrame, metaclass=CardoWrapper):
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def __getattribute__(self, item):
        return _get_attribute(self, item)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._df))


class CardoRDD(RDD, metaclass=CardoWrapper):
    def __init__(self, rdd: RDD):
        self._rdd = rdd

    def __getattribute__(self, item):
        return _get_attribute(self, item, _RDD)

    def to_cardo_dataframe(self, session: SparkSession):
        return CardoDataFrame(session.createDataFrame(self._df))
