#!/usr/bin/env bash

mvn install:install-file \
   -Dfile=./app/core/target/scala-2.11/core_2.11-0.4.0.jar \
   -DgroupId=com.qwery \
   -DartifactId=core_2.11 \
   -Dversion=0.4.0 \
   -Dpackaging=jar \
   -DgeneratePom=true

mvn install:install-file \
   -Dfile=./app/util/target/scala-2.11/util_2.11-0.4.0.jar \
   -DgroupId=com.qwery \
   -DartifactId=util_2.11 \
   -Dversion=0.4.0 \
   -Dpackaging=jar \
   -DgeneratePom=true

mvn install:install-file \
   -Dfile=./app/language/target/scala-2.11/language_2.11-0.4.0.jar \
   -DgroupId=com.qwery \
   -DartifactId=language_2.11 \
   -Dversion=0.4.0 \
   -Dpackaging=jar \
   -DgeneratePom=true

mvn install:install-file \
   -Dfile=./app/platform/spark/generator/target/scala-2.11/spark-generator_2.11-0.4.0.jar \
   -DgroupId=com.qwery \
   -DartifactId=spark-generator_2.11 \
   -Dversion=0.4.0 \
   -Dpackaging=jar \
   -DgeneratePom=true

mvn install:install-file \
   -Dfile=./app/platform/spark/tools/target/scala-2.11/spark-tools_2.11-0.4.0.jar \
   -DgroupId=com.qwery \
   -DartifactId=spark-tools_2.11 \
   -Dversion=0.4.0 \
   -Dpackaging=jar \
   -DgeneratePom=true