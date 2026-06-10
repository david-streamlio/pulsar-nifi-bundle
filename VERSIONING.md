# Versioning & Compatibility

This bundle follows the versioning convention established by Apache NiFi's
official Kafka connectors, adapted for a third-party connector.

## The rule

> **The NiFi platform release version owns the Maven `<version>`.**
> **The Pulsar client version is a property of the release line, recorded in
> the compatibility matrix below — not encoded in the artifact coordinate.**

This mirrors NiFi 2.x's *consolidated* Kafka bundle (`nifi-kafka-nar`,
`PublishKafka`/`ConsumeKafka`, one Kafka client generation per release line),
as opposed to the older NiFi 1.x multi-bundle scheme that shipped
`nifi-kafka-2-0-nar`, `nifi-kafka-2-6-nar`, … side by side. We chose the
consolidated model because we support **one Pulsar client major per release
line** rather than multiple Pulsar majors simultaneously.

## Version string

```
<nifi.major>.<nifi.minor>.<nifi.patch>[.<connectorRevision>]
```

- The first three segments are the **NiFi version this artifact is built for**.
- The optional fourth segment is a **connector revision** — incremented when we
  ship a connector-only change (bug fix, dependency bump, Pulsar *minor* upgrade)
  against the *same* NiFi version. Maven orders these correctly:
  `2.1.0 < 2.1.0.1 < 2.1.0.2 < 2.2.0`.

| Example | Meaning |
|---|---|
| `2.1.0` | First release built for NiFi 2.1.0 |
| `2.1.0.1` | Connector fix/dep bump, still NiFi 2.1.0 |
| `2.2.0` | First release built for NiFi 2.2.0 |

The Maven `<version>` in `pom.xml` is the single source of truth for the
release number. There is no separate `version` file.

## Pulsar client major

Because the artifact coordinate does **not** encode the Pulsar major (consolidated
model), a given release line targets exactly one Pulsar client major. A change of
Pulsar major (e.g. 3.x → 4.x) is a significant event and must be:

1. tied to a NiFi-version bump where possible, and
2. always announced in the release notes and reflected in the matrix below.

Consumers who must stay on a specific Pulsar major should pin to the matching
release line documented below.

## Branching

Mirrors apache/nifi's `support/nifi-N.x` model:

- **`main`** — active development against the newest supported NiFi line and its
  chosen Pulsar major.
- **`support/nifi-<X.Y>`** — maintenance branches for older NiFi lines.

## Release tags

Releases are cut by pushing a tag `v<version>` (e.g. `v2.1.0`, `v2.1.0.1`), which
triggers `.github/workflows/release.yml` to build the NARs and publish a GitHub
Release. The tag's numeric part must equal the pom `<version>`.

## Compatibility matrix

| Bundle version | NiFi | Pulsar client | Java |
|---|---|---|---|
| `2.1.0` | 2.1.0 | 3.3.7 | 21 |

> **Note on history:** releases before `2.1.0` used an independent connector
> semver (last tag `v1.5.0`). Starting with `2.1.0` the version is re-anchored to
> the NiFi platform version per the rule above; this is a numbering re-anchor, not
> twenty releases of new work.
