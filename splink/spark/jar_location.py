def similarity_jar_location():
    import pyspark as spark
    from packaging import version

    import splink

    if version.parse(spark.__version__) >= version.parse("3.0"):
        path = (
            splink.__file__[0:-11]
            + "files/spark_jars/scala-udf-similarity-0.1.1_spark3.x.jar"
        )
    else:
        path = (
            splink.__file__[0:-11]
            + "files/spark_jars/scala-udf-similarity-0.1.0_legacy.jar"
        )

    return path
