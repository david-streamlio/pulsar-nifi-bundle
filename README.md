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

### Record sets and the record schema

`ConsumePulsarRecord` writes consecutive messages with the same mapped attributes (and topic)
as one record set, using the schema the set was opened with. A message whose schema differs
starts a new record set - and a new FlowFile - just as a change in the mapped attributes does.

> **Behaviour change since `2.9.0`:** the record set used to be written with the schema of its
> **first** message. With a Record Reader that infers the schema from each message (the default
> of `JsonTreeReader`), every field that the first message of a batch did not have was silently
> dropped from the rest of the batch, and a field whose type differed made the trigger fail. Such
> batches are now split at every schema change instead, so a topic with payloads of several
> shapes produces more, smaller FlowFiles than before. If that matters more than the optional
> fields, give the reader an explicit schema (*Schema Text* or a schema registry): every message
> then resolves to the same schema and the batch stays one FlowFile.

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

## Message routing on partitioned topics

*Message Routing Mode* decides where an **unkeyed** message goes on a partitioned topic:
`RoundRobinPartition` (the default) spreads them over the partitions, `SinglePartition` keeps
them on one partition chosen per producer. A message with a key is hashed to a partition in
either mode, so keyed messages keep their order per key regardless of the setting.
`CustomPartition` needs a `MessageRouter` the processors cannot configure and is rejected at
validation. *Max Pending Messages* bounds the producer's queue of messages awaiting the
broker's acknowledgement.

> **Behaviour change since `2.9.0`:** neither property reached the producer since the publish
> processors were refactored in 2023 — the producer always ran with the client defaults. A flow
> that has *Message Routing Mode* set to `SinglePartition` will now really route its unkeyed
> messages to a single partition. A flow that had `CustomPartition` selected becomes invalid and
> has to pick one of the other two modes.
>
> *Max Pending Messages* now applies **to every flow, including one that never configured it**.
> The property has always shown a default of `1000`, but the value never reached the client, which
> ran with the count-based bound disabled. With *Async Enabled*, messages are published through
> `sendAsync()` and count against that bound; when it is reached and *Block if Message Queue Full*
> is off — the default — a send fails with `ProducerQueueIsFullError` and its FlowFile is routed to
> `failure` rather than waiting for room.
>
> Whether a flow reaches the bound depends on how fast the broker acknowledges, so it is likelier
> against a remote or loaded broker than in testing. If you publish large FlowFiles asynchronously,
> either raise *Max Pending Messages*, set it to `0` to restore the previous unbounded behaviour, or
> enable *Block if Message Queue Full* so sends wait instead of failing.

## Consuming from topics that have a schema

`ConsumePulsarRecord`'s **Message Schema Strategy** decides how a message becomes records.

`Record Reader` (the default) parses each message with the configured reader, which has to match how the
topic is encoded. That is not always obvious: an AVRO topic carries *bare Avro binary* — no file header
and no embedded schema, because Pulsar keeps the schema in its registry — so it needs an `AvroReader`
whose *Schema Access Strategy* is `Use 'Schema Text' Property` and whose *Schema Text* is
`${avro.schema}`, the attribute this processor sets from the topic's registered schema. A JSON topic
carries text a `JsonTreeReader` can infer. Pointing the wrong reader at a topic sends every message to
`parse.failure`.

`Topic Schema` builds records from the schema the topic carries instead. The field definitions come from
the broker — Pulsar attaches each message's schema to it — so no reader and no schema configuration are
needed at all, and AVRO and JSON topics behave identically. Because the schema arrives per message,
evolution is handled: a message published under an older version decodes with the version it was written
with.

A *Record Reader* may still be configured under `Topic Schema`, and messages the strategy cannot decode
fall back to it — a topic with no schema at all, or one whose schema has no record shape. Without a
reader to fall back to, those messages go to `parse.failure`. Primitive and `KeyValue` schemas are not
yet decoded this way.

## Publishing to topics that have a schema

`PublishPulsar` and `PublishPulsarRecord` create their producers with
`Schema.AUTO_PRODUCE_BYTES()`, so the broker validates every payload against the schema the
topic currently carries.

| Topic | Behaviour |
|---|---|
| no schema | any content is accepted, exactly as before |
| has a schema, content matches | published normally |
| has a schema, content does not match | **routed to `failure`** with the broker's error |

> **Breaking change.** Until now the processors published with `Schema.BYTES`, which the broker
> does not validate. Content that did not match the topic's schema was accepted anyway: the
> message landed on the topic looking valid, the registered schema was left untouched, and the
> problem only appeared later, on the consumer side. A schema-aware consumer would fail to decode
> that message — and, because it could not get past it, stop consuming the topic entirely:
>
> ```
> read 1: id=sensor-1 reading=42
> read 2 FAILED: AvroRuntimeException: Malformed data. Length is negative: -62
> ```
>
> **Flows that publish content not matching their topic's schema will start routing those
> FlowFiles to `failure`.** They were previously reported as successful while producing messages
> no consumer could read, so this surfaces an existing problem rather than creating one — but it
> is a visible change in behaviour, and it needs a `failure` connection to be handled.
>
> Only topics that carry a schema are affected. If your topics have no schema — the default, and
> what every flow using these processors has relied on so far — nothing changes.
>
### Publishing records to a schema-bearing topic

`PublishPulsarRecord` has a **Message Schema Strategy** property controlling how records become messages:

| Strategy | Behaviour |
|---|---|
| `Record Writer` (default) | serialize with the configured Record Writer, as before |
| `Topic Schema` | convert each record to the topic's Avro schema and encode it the way Pulsar does |

Use `Topic Schema` when the topic carries an AVRO schema: the Record Writer's output — JSON, CSV,
Avro-with-header — is not what the broker accepts, so it is rejected. On a topic with no Avro
schema this strategy falls back to the Record Writer, so turning it on is safe either way.

`Topic Schema` encodes for both **AVRO** and **JSON** topic schemas.

> **Worth knowing about JSON-schema topics:** the broker validates payloads against an AVRO schema
> and rejects content that does not match, but it does **not** do the same for a JSON schema. Before
> this, publishing to a JSON-schema topic with the Record Writer put content on the topic that a
> schema-aware consumer decoded as all-null fields, with nothing reported at either end. `Topic
> Schema` is the only strategy that produces messages such a consumer can read.

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
