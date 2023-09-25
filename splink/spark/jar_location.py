from pyspark.sql.types import DoubleType, StringType


def _spark_v_3_check() -> bool:
    """Check if spark 3.0 or above is installed

    Returns:
        bool: True if pyspark >= 3.0.0
    """
    import pyspark as spark
    from packaging import version

    return version.parse(spark.__version__) >= version.parse("3.0")


def similarity_jar_location():
    import splink

    if _spark_v_3_check():
        path = (
            splink.__file__[0:-11]
            + "files/spark_jars/scala-udf-similarity-0.1.1_spark3.x.jar"
        )
    else:
        path = (
            splink.__file__[0:-11]
            + "files/spark_jars/scala-udf-similarity-0.1.0_classic.jar"
        )

    return path


def get_scala_udfs():
    # This functions expects that either:
    # 1) Runtime is within a DataBricks environment, in which case jar
    # registration is taken care of behind the scenes.
    # 2) The user has registered the jar when configuring their spark
    # session.

    # register udf functions
    # will for loop through this list to register UDFs.
    # List is a tuple of structure (UDF Name, class path, spark return type)
    udfs_register = [
        ("jaro_sim", "uk.gov.moj.dash.linkage.JaroSimilarity", DoubleType()),
        (
            "jaro_winkler",
            "uk.gov.moj.dash.linkage.JaroWinklerSimilarity",
            DoubleType(),
        ),
        ("jaccard", "uk.gov.moj.dash.linkage.JaccardSimilarity", DoubleType()),
        ("cosine_distance", "uk.gov.moj.dash.linkage.CosineDistance", DoubleType()),
        ("Dmetaphone", "uk.gov.moj.dash.linkage.DoubleMetaphone", StringType()),
        (
            "DmetaphoneAlt",
            "uk.gov.moj.dash.linkage.DoubleMetaphoneAlt",
            StringType(),
        ),
        ("QgramTokeniser", "uk.gov.moj.dash.linkage.QgramTokeniser", StringType()),
    ]

    if _spark_v_3_check():
        # Outline spark 3 exclusive scala functions
        spark_3_udfs = [
            (
                "damerau_levenshtein",
                "uk.gov.moj.dash.linkage.LevDamerauDistance",
                DoubleType(),
            ),
        ]
        # Register spark 3 excl. functions
        udfs_register.extend(spark_3_udfs)

    return udfs_register
