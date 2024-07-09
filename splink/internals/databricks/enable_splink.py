import logging
import os

from splink.internals.spark.jar_location import similarity_jar_location

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

    dbr_version = float(os.environ.get("DATABRICKS_RUNTIME_VERSION", "0.0"))

    try:
        if dbr_version >= 14:
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
                optionModule.apply(None),
                optionModule.apply(None),
                optionModule.apply(None),
            )
        elif dbr_version >= 13:
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
                optionModule.apply(None),
                optionModule.apply(None),
            )
        else:
            lib = JavaJarId(
                JarURI,
                ManagedLibraryId.defaultOrganization(),
                NoVersionModule.simpleString(),
            )
    except Exception as e:
        logger.warn("failed to enable similarity jar functions for Databricks", e)

    libSeq = converters.asScalaBufferConverter((lib,)).asScala().toSeq()

    context = DatabricksILoop.getSharedDriverContextIfExists().get()
    context.registerNewLibraries(libSeq)
    context.attachLibrariesToSpark(libSeq)
