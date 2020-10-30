from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext, SparkConf
from pyspark.sql import SQLContext

from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, Row
import pyspark.sql.functions as f
from pyspark.sql.functions import when


def vif_gammas(inputdata, sparksession, sampleratio=1.0):
    collist = []
    viflist = []

    sc = sparksession.sparkContext
    sqlContext = SQLContext(sc)

    dfvariables = inputdata.columns

    # get gamma_ columms only

    gammaonly = [s for s in dfvariables if str(s).startswith("gamma")]

    # if no gamma columns available exit function gracefully
    if gammaonly == []:
        print("not any probability columns present")
        emptyschema = StructType([StructField("", StringType(), True)])
        return sqlContext.createDataFrame([], emptyschema)

    # only keep gamma_ columns
    inputdata = inputdata.select(gammaonly).sample(
        withReplacement=False, fraction=sampleratio, seed=42
    )

    # cast gamma_ columns to double in case they are not
    inputdata = inputdata.select(*(f.col(c).cast("double") for c in inputdata.columns))

    # clamp values to either 0 or 1 or NULL in case of -1

    for gammacol in gammaonly:
        inputdata = inputdata.withColumn(
            gammacol,
            f.when(inputdata[gammacol] < 0.0, None)
            .when(inputdata[gammacol] > 0.0, 1.0)
            .otherwise(0.0),
        )

    # drop any NULLs
    inputdata = inputdata.na.drop()
    # add a dummy unused column in the start of the dataframe to make the round robin thing work on the vif calcs.
    inputdata = inputdata.withColumn("_", f.lit("_")).select("_", *gammaonly)
    vifcols = inputdata.columns

    # VIF computation

    for i in range(1, len(vifcols)):

        # round robin computation of r_squared and vif from the available vars
        train_t = inputdata.rdd.map(
            lambda x: [Vectors.dense(x[2:i] + x[i + 1 :]), x[i]]
        ).toDF(["features", "label"])

        lr = LinearRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(train_t)
        predictions = lr_model.transform(train_t)
        evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label")
        r_sq = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        vif = 1.0 / (1.0 - r_sq)

        collist.append(vifcols[i])
        viflist.append(vif)

    vifSchema = StructType(
        [StructField("col", StringType()), StructField("vif", DoubleType())]
    )

    return sqlContext.createDataFrame(zip(collist, viflist), vifSchema)
