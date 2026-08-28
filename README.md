# Apache NiFi - Processor for Apache Pulsar

## Compatibility

| Bundle version | NiFi | Pulsar client | Java |
|---|---|---|---|
| `2.9.0` | 2.9.0 | 4.2.2 | 21 |
| `2.1.0` | 2.1.0 | 3.3.7 | 21 |

> **Pulsar major bump in `2.9.0`:** this release line moves the Pulsar client
> from `3.x` to `4.x` (3.3.7 → 4.2.2). Consumers who must stay on the Pulsar 3.x
> client should pin to the `2.1.0` release line.

The bundle version tracks the NiFi platform version it is built for; each release
line targets one Pulsar client major. See [VERSIONING.md](VERSIONING.md) for the
full scheme, branching model, and release process.

## How to build

To build the NAR files using Maven, just run the following commands. The first one makes sure that you are using Java 
version 21, which is necessary since NiFi 2.x uses this version.

```
export JAVA_HOME=`/usr/libexec/java_home -v 21`
mvn clean package
```

This will also generate a Docker image inside your local docker daemon with the tag `streamnative/nifi`

*Note: Currently, this command will load NAR files that were build using the default NiFi, Pulsar, and Java versions
into the lib folder of the NiFi container for testing. Therefore, if you need to test artifacts built using a
different version of these libraries, then you will first need to copy those NAR artifacts into the docker/lib folder *BEFORE* building
the Docker image.

## How to test

A Dockerfile has been included in the project that can be used to test the Processor locally, and can be started with the following command:

```
docker run --name nifi -d -p 8443:8443 \
-e SINGLE_USER_CREDENTIALS_USERNAME=admin \
-e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB \
streamnative/nifi
```

See the [documentation](https://hub.docker.com/r/apache/nifi) on the base image for more configuration options

Visit https://localhost:8443/nifi/#/login and enter the username and password you provided in the docker command.

## Integration tests

Alongside the unit tests (which run against a mocked Pulsar client) there are integration tests
that drive the processors against a **real Pulsar broker**, started in Docker by
[Testcontainers](https://java.testcontainers.org/). They cover behaviour the mocks cannot reach:
real message ids, real acknowledgement semantics, subscription types and partitioned topics.

They are named `*IT.java`, so Surefire ignores them - `mvn test` and `mvn package` stay fast and
need no Docker. They run from `mvn verify`:

```
mvn verify                 # unit tests + integration tests (needs Docker)
mvn verify -DskipITs       # unit tests only
mvn test                   # unit tests only, no Docker required
```

The broker image is pinned by the `pulsar.image` property in the root pom and kept in step with
`pulsar.version`.

> **Docker 29 and newer:** docker-java (bundled with Testcontainers) negotiates API version 1.32 by
> default, which Docker 29+ rejects with *"client version 1.32 is too old. Minimum supported API
> version is 1.44"*. The build therefore passes `-Dapi.version=${docker.api.version}` (1.44) to the
> integration tests. 1.44 requires Docker 25 or newer; on an older daemon override it, e.g.
> `mvn verify -Ddocker.api.version=1.41`.

## How to debug

The JVM Debugger can be enabled by setting the environment variable NIFI_JVM_DEBUGGER to any value when running the docker image, e.g.

```
docker run -d --name nifi \
-v /Users/david/Downloads/nifi-test/:/nifi-test
-p 8443:8443 -p 8000:8000 \
-e NIFI_JVM_DEBUGGER=true
-e SINGLE_USER_CREDENTIALS_USERNAME=admin
-e SINGLE_USER_CREDENTIALS_PASSWORD=ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB
streamnative/nifi
```

## References
https://stackoverflow.com/questions/55811413/is-it-possible-to-debug-apache-nifi-custom-processor
