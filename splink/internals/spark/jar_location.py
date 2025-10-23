import pyspark as spark
from pyspark.sql.types import DoubleType, StringType


def _spark_major_version() -> int:
    """Check which spark major version is installed

    Returns:
        int: e.g. pyspark == 3.0.0 returns 3
    """

    major, _minor, _patch = spark.__version__.split(".")
    return int(major)


def similarity_jar_location() -> str:
    import splink

    spark_major_version = _spark_major_version()

    if spark_major_version >= 4:
        path = (
            splink.__file__[0:-11]
            + "internals/files/spark_jars/scala-udf-similarity-0.2.0_spark4.x.jar"
        )
    elif spark_major_version == 3:
        path = (
            splink.__file__[0:-11]
            + "internals/files/spark_jars/scala-udf-similarity-0.1.2_spark3.x.jar"
        )
    else:
        path = (
            splink.__file__[0:-11]
            + "internals/files/spark_jars/scala-udf-similarity-0.1.0_classic.jar"
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

    if _spark_major_version() >= 3:
        # Outline spark 3+ exclusive scala functions
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
