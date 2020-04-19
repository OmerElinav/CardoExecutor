import pandas as pd
from pyspark import RDD
from pyspark.sql import DataFrame

from CardoExecutor.Common.Tests.CardoTestCase import CardoTestCase
from CardoExecutor.Common.CardoDataFrame import CardoDataFrame, CardoRDD, CardoPandasDataFrame


class CardoDataFrameTests(CardoTestCase):
    def test_cast_to_rdd(self):
        # Arrange
        df = CardoDataFrame(self.context.spark.createDataFrame([[1]]))
        rdd = self.context.spark.createDataFrame([[1]]).rdd

        # Act
        df = df.to_cardo_rdd()

        # Assert
        self.assertEqual(df.collect(), rdd.collect())
        self.assertIsInstance(df, RDD)

    def test_cast_to_pandas(self):
        # Arrange
        df = CardoDataFrame(self.context.spark.createDataFrame([[1]]))
        # pd_df = self.context.spark.createDataFrame([[1]]).toPandas()
        pd_df = pd.DataFrame([1], columns=["_1"])

        # Act
        df = df.to_cardo_pandas()

        # Assert
        self.assertTrue(df.equals(pd_df), msg="dataframes not equal")

    def test_rdd_to_spark(self):
        # Arrange
        rdd = CardoRDD(self.context.spark.createDataFrame([[1]]).rdd)
        df = self.context.spark.createDataFrame([[1]])

        # Act
        rdd = rdd.to_cardo_dataframe(self.context.spark)

        # Assert
        self.assertEqual(rdd.collect(), df.collect())
        self.assertIsInstance(rdd, DataFrame)

    def test_pandas_to_spark(self):
        # Arrange
        pd_df = CardoPandasDataFrame(pd.DataFrame([1], columns=["_1"]))
        df = self.context.spark.createDataFrame([[1]])

        # Act
        pd_df = pd_df.to_cardo_dataframe(self.context.spark)

        # Assert
        self.assertEqual(pd_df.collect(), df.collect())
        self.assertIsInstance(pd_df, DataFrame)

    def test_chain_methods(self):
        # Arrange
        df = CardoDataFrame(self.context.spark.createDataFrame([[1]]))

        # Act
        df = df.repartition(1)

        # Assert
        self.assertIsInstance(df, CardoDataFrame)
