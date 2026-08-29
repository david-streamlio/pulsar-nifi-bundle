# Contributing

Thanks for helping out. This is a short guide to the things that are easy to get wrong in this
repository specifically — the general Git and GitHub etiquette you already know is assumed.

## Before you start writing code

**Check whether someone is already on it.** Look for an open issue, and for an open pull request
that references it. Two contributors have independently written the same fix here more than once;
both times the work was good and one of the two had to be thrown away.

If you are picking something up, say so on the issue and assign yourself. If you are *filing* an
issue that you already intend to fix, say that in the issue body — that is the moment someone else
decides whether to start.

## Tests

The unit tests run against a mocked Pulsar client. They are fast and cover a lot, but there is a
whole class of behaviour they cannot reach: acknowledgement semantics, subscription types,
partitioned topics, schemas, and anything the broker itself decides. Several bugs reached releases
because the mock returned a `null` message id, or a fixed schema, where a real broker would not.

So there are two layers:

```
mvn test                   # unit tests. Fast, no Docker.
mvn verify                 # unit + integration tests. Needs Docker.
mvn verify -DskipITs       # unit only, from a verify build
```

Integration tests are named `*IT.java` and run against a real Pulsar broker started by
Testcontainers. Surefire ignores the `*IT` suffix, so `mvn test` and `mvn package` stay fast and
need no Docker; Failsafe runs them from `mvn verify`.

**If you are changing anything that depends on broker behaviour, add or extend an integration
test.** If you cannot run them locally, say so in the pull request so a reviewer knows to check.

### Docker 29 and newer

docker-java, which Testcontainers uses, negotiates API version 1.32 by default, and Docker 29+
rejects that outright:

```
client version 1.32 is too old. Minimum supported API version is 1.44
```

It surfaces as a misleading `Could not find a valid Docker environment`. The build passes
`-Dapi.version=${docker.api.version}` (1.44) to work around it. 1.44 needs Docker 25 or newer; on
an older daemon, override it:

```
mvn verify -Ddocker.api.version=1.41
```

## Writing a fix

**Make the test fail first.** Every fix here should come with a test that fails against the code
before the change and passes after. Run it both ways and put the failure output in the pull
request — it is the difference between "this looks right" and "this was wrong, and here is the
proof". Several changes in this repository looked correct and were not; the ones that held up were
the ones with a red test behind them.

**Assert the invariant, not a number.** A test that asserts "at most 3 items remain" encodes a
guess. A test that asserts "the count does not grow with the number of iterations" encodes the
thing you actually mean, and does not go flaky under load.

**A flaky-looking failure deserves reading before re-running.** One intermittent failure here was
a genuine lost message — `Wanted 1000 times ... But was 999` — dismissed as flakiness and re-run
past. Read the message first.

## Pull requests

Keep them to one concern. A CI fix does not belong in a feature branch, and a refactor does not
belong in a bug fix. If you find a second bug while fixing the first — which happens often in this
codebase — file it separately and say so.

Say what you did *not* do, as well as what you did: cases you did not cover, tests you could not
run, assumptions you did not verify. That is the part reviewers cannot see from the diff.

## Behaviour changes

Some fixes here necessarily change what existing flows do — content that used to be published is
now rejected, FlowFiles that used to reach `success` now reach `failure`. When that happens, add a
callout to the README next to the affected section, saying plainly what changes and what a user
should do about it. Look for the existing "Behaviour change since 2.9.0" notes for the shape.
