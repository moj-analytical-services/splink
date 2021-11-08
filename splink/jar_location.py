def similarity_jar_location():
    import splink

    path = splink.__file__[0:-11] + "jars/scala-udf-similarity-0.0.9.jar"
    return path