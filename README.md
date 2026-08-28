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

## Consumer FlowFile attributes

`ConsumePulsar` and `ConsumePulsarRecord` write these attributes onto every FlowFile
they emit:

| Attribute | Written by | Value |
|---|---|---|
| `message.count` | `ConsumePulsar` | number of Pulsar messages in the FlowFile |
| `record.count` | `ConsumePulsarRecord` | number of records in the FlowFile |
| `pulsar.message.id` | both | the message id — **only when the FlowFile holds exactly one message** |
| `pulsar.message.id.first` | both | id of the first message in the FlowFile |
| `pulsar.message.id.last` | both | id of the last message in the FlowFile |
| `pulsar.property.*` | both | message properties, prefixed — **only those whose value is identical in every message** |
| `topicName` | `ConsumePulsarRecord` | the logical topic the messages came from |
| `avro.schema` | `ConsumePulsarRecord` | the topic schema, when it is an AVRO schema |

A FlowFile can hold several messages: *Consumer Message Batch Size* controls how many.
Consecutive messages are appended to the same FlowFile as long as their *Mapped FlowFile
Attributes* are identical — a change in any mapped value starts a new FlowFile before the
batch size is reached. Per-message metadata (the message id, unmapped message properties)
deliberately does **not** take part in that decision, so it never splits a batch.

To force messages that differ in some property into separate FlowFiles, map that property
through *Mapped FlowFile Attributes*.

> **Attribute change since `2.9.0`:** `pulsar.message.id` and the full set of
> `pulsar.property.*` used to appear on every FlowFile, because a bug made each FlowFile
> hold exactly one message regardless of *Consumer Message Batch Size*. Now that batching
> works, a FlowFile that holds more than one message has no single message id, so
> `pulsar.message.id` is omitted there and only the properties common to the whole batch
> are set. Flows that read `${pulsar.message.id}` downstream should use
> `${pulsar.message.id.first}` / `${pulsar.message.id.last}`, or set *Consumer Message
> Batch Size* to `1` to keep one message per FlowFile.
>
> For the same reason, `pulsar.property.*` values are no longer available to
> `ConsumePulsarRecord`'s Schema Access Strategy: they are attached once the batch is
> complete, which is after the record reader and writer have been created. A schema name
> that comes from a message property has to be mapped through *Mapped FlowFile Attributes*
> instead.

## Publisher message metadata

`PublishPulsar` and `PublishPulsarRecord` set the message key and message properties from the
FlowFile:

| Message field | Comes from |
|---|---|
| key | the *Message Key* property; if that is not set, the FlowFile attribute `msg.key` |
| properties | the attributes named by *Mapped Message Properties* (`<property>[=<attribute>]`) |

`PublishPulsarRecord` takes the key from the record field named by *Message Key Field* instead.

> **Behaviour change since `2.9.0`:** the *Message Key* property has always documented the
> `msg.key` fallback, but it was never implemented — `getMessageKey()` read the property and
> returned nothing when it was blank. Flows that set a `msg.key` attribute without setting the
> property therefore published **unkeyed** messages. That fallback now works as documented, so
> those flows will start producing keyed messages. On a partitioned topic this changes which
> partition a message routes to, and it makes the topic compactable by that key. If you relied on
> the previous unkeyed behaviour, clear the `msg.key` attribute before the publish processor.

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
