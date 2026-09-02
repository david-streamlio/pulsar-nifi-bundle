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

### Where the version lives

The **git tag is authoritative** for a release. The pom `<version>` on `main`
stays at the NiFi base version (e.g. `2.1.0`); you do **not** hand-edit it or
commit a bump before tagging. When you push a tag `vX.Y.Z[.R]`, the release
workflow stamps the pom from the tag (`mvn versions:set -DnewVersion=X.Y.Z[.R]`)
before building, so the NARs and the Docker image are labeled to match the tag.
There is no separate `version` file.

This keeps the release flow to a single action — push a tag — with no
version-bump commit and no chance of the artifacts disagreeing with the tag.

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
- **`support/nifi-N.x`** — maintenance branches, one per NiFi **major** line.

> This bullet previously read `support/nifi-<X.Y>`, which contradicted the sentence
> above it: that pattern is one branch per NiFi *minor* (`support/nifi-2.11`), where
> apache/nifi's model — and the sentence — is one per *major* (`support/nifi-2.x`).
> No support branch existed while the two disagreed, so nothing was built on the wrong
> one; the branch that now exists follows the major-line model.

### `support/nifi-2.x`

Cut from the `v2.11.0` tag. It carries the 2.x line — NiFi 2.x, Pulsar client 4.x — for
as long as that line is maintained.

It exists because `main` is expected to diverge: the restructure in #188 changes the
controller service interface and the processor/service split, which is a breaking change
for existing flows and is targeted at 3.0. Once that lands, `main` is no longer releasable
as 2.x, and this branch is where 2.x fixes and connector revisions (`2.11.0.1`, `2.11.1`,
…) are cut from.

It was cut at the release rather than when the restructure starts, because a support branch
is cheapest to create from a tag whose contents are known good, and reconstructing one after
`main` has moved is not.

## Release tags

Releases are cut by pushing a tag `v<version>` (e.g. `v2.1.0`, `v2.1.0.1`), which
triggers `.github/workflows/release.yml` to stamp the pom version from the tag,
build the NARs, publish a GitHub Release, and push the Docker image to GHCR
(`ghcr.io/<owner>/nifi:<version>` and `:latest`). The tag is the single source of
truth — nothing needs to be committed beforehand.

## Compatibility matrix

| Bundle version | NiFi | Pulsar client | Java |
|---|---|---|---|
| `2.11.0` | 2.11.0 | 4.2.4 | 21 |
| `2.10.0` | 2.10.0 | 4.2.4 | 21 |
| `2.9.0` | 2.9.0 | 4.2.2 | 21 |
| `2.1.0` | 2.1.0 | 3.3.7 | 21 |

> **Pulsar major bump in `2.9.0`:** this release line moves the Pulsar client
> from `3.x` to `4.x` (3.3.7 → 4.2.2). Consumers who must stay on the Pulsar 3.x
> client should pin to the `2.1.0` release line.

> **Note on history:** releases before `2.1.0` used an independent connector
> semver (last tag `v1.5.0`). Starting with `2.1.0` the version is re-anchored to
> the NiFi platform version per the rule above; this is a numbering re-anchor, not
> twenty releases of new work.
