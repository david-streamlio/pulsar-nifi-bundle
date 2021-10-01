# Apache NiFi - Processor for Apache Pulsar

## How to build

This processor allows you to define the versions of the Apache NiFi and Apache Pulsar libraries that you that you want to use inside the processor, along with the JDK version you want to compile the classes with. 

To build the NAR files using Maven, just run the following command:

`mvn clean package -Dnifi.version=<NIFI VERSION> -Dulsar.version=<PULSAR VERSION> -Djdk.release=<JAVA VERSION>`

The default values for these properties are, 1.11.3 for NiFi, 2.8.0 for Pulsar, and JDK 8 for Java. Note that currently NiFi version 1.14 is not currently supported due to an API change.

## How to test

A Dockerfile has been included in the project that can be used to test the Processor locally, and can be built using the following command:

`docker build -t <TAG> .`

Currently this command will load NAR files that were build using the default NiFi, Pulsar, and Java versions into the lib folder of the NiFi container for testing. Therefore, if you need to test artifacts built using a different version of these libraries, then you will first need to copy those NAR artifacts into the docker/lib folder *BEFORE* building the Docker image.