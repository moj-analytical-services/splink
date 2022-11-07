from splink.databricks.jar_location import similarity_jar_location

def enable_splink(spark):
    sc = spark.sparkContext
    _jar_path = similarity_jar_location()
    JavaURI = getattr(sc._jvm.java.net, "URI")
    JavaJarId = getattr(sc._jvm.com.databricks.libraries, "JavaJarId")
    ManagedLibraryId = getattr(
        sc._jvm.com.databricks.libraries, "ManagedLibraryId"
    )
    ManagedLibraryVersions = getattr(
        sc._jvm.com.databricks.libraries, "ManagedLibraryVersions"
    )
    NoVersion = getattr(ManagedLibraryVersions, "NoVersion$")
    NoVersionModule = getattr(NoVersion, "MODULE$")
    DatabricksILoop = getattr(
        sc._jvm.com.databricks.backend.daemon.driver, "DatabricksILoop"
    )
    converters = sc._jvm.scala.collection.JavaConverters

    JarURI = JavaURI.create("file:" + _jar_path)
    lib = JavaJarId(
        JarURI,
        ManagedLibraryId.defaultOrganization(),
        NoVersionModule.simpleString(),
    )
    libSeq = converters.asScalaBufferConverter((lib,)).asScala().toSeq()

    context = DatabricksILoop.getSharedDriverContextIfExists().get()
    context.registerNewLibraries(libSeq)
    context.attachLibrariesToSpark(libSeq)