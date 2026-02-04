import pyspark as spark


def get_spark_major_version() -> int:
    """Check which spark major version is installed

    Returns:
        int: e.g. pyspark == 3.0.0 returns 3
    """

    major_str = spark.__version__.split(".", 1)[0]
    return int(major_str)
