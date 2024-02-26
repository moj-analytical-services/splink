import logging

from splink.spark.jar_location import similarity_jar_location

logger = logging.getLogger(__name__)


def enable_splink(spark):
    """
    Enable Splink functions.
    Use this function at the start of your workflow to ensure Splink is registered on
    your Databricks cluster.

    Args:
        spark (pyspark.sql.SparkSession): The active SparkSession.
    Returns:
        None
    """
    sc = spark.sparkContext
    _jar_path = similarity_jar_location()

    JavaURI = sc._jvm.java.net.URI
    JavaJarId = sc._jvm.com.databricks.libraries.JavaJarId
    ManagedLibraryId = sc._jvm.com.databricks.libraries.ManagedLibraryId
    ManagedLibraryVersions = sc._jvm.com.databricks.libraries.ManagedLibraryVersions
    NoVersion = getattr(ManagedLibraryVersions, "NoVersion$")
    NoVersionModule = getattr(NoVersion, "MODULE$")
    DatabricksILoop = sc._jvm.com.databricks.backend.daemon.driver.DatabricksILoop
    converters = sc._jvm.scala.collection.JavaConverters

    JarURI = JavaURI.create("file:" + _jar_path)
    optionClass = getattr(sc._jvm.scala, "Option$")
    optionModule = getattr(optionClass, "MODULE$")

    # Note(bobby): So dirty
    try:
        # This will fix the exception when running on Databricks Runtime 14.x+
        lib = JavaJarId(
            JarURI,
            ManagedLibraryId.defaultOrganization(),
            NoVersionModule.simpleString(),
            optionModule.apply(None),
            optionModule.apply(None),
            optionModule.apply(None),
        )
    except Exception as e:
        logger.warn("failed to initialize for 14.x+", e)
        try:
            # This will fix the exception when running on Databricks Runtime 13.x
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
                optionModule.apply(None),
                optionModule.apply(None),
            )
        except Exception as ex:
            logger.warn("failed to initialize for 13.x", ex)

            # This will work for < 13.x
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
            )

    libSeq = converters.asScalaBufferConverter((lib,)).asScala().toSeq()

    context = DatabricksILoop.getSharedDriverContextIfExists().get()
    context.registerNewLibraries(libSeq)
    context.attachLibrariesToSpark(libSeq)
