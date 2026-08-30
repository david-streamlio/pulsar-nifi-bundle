# Security Policy

## Supported versions

Security fixes are applied to the most recent release line only. The bundle version
tracks the NiFi platform version it is built for (see the compatibility table in the
[README](README.md)).

| Bundle version | Supported |
|---|---|
| `2.10.x` | Yes |
| `2.9.x` | No — upgrade to `2.10.x` |
| `2.1.x` and earlier | No |

If you are pinned to an older line because of a platform constraint, say so in your
report; it helps to know that a backport matters to someone.

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report privately through GitHub:

1. Go to the [Security tab](../../security) of this repository.
2. Click **Report a vulnerability**.
3. Describe the issue, the affected version, and how to reproduce it.

That opens a private advisory thread visible only to you and the maintainers. If you
cannot use GitHub's private reporting for any reason, open a public issue containing
only a request for a private contact channel — no details.

Helpful things to include, when you have them:

- The affected version, and whether you reproduced against the NARs or the container image
- A minimal reproduction — a flow definition, processor configuration, or test case
- What an attacker gains, and what access they need to get it

## What to expect

This is a small project maintained on a best-effort basis. Concretely:

- **Acknowledgement within 5 business days.** If you have not heard back by then, assume
  the notification was missed and ping the thread.
- An initial assessment — whether it reproduces, and whether it is in scope — within
  two weeks of acknowledgement.
- Progress updates as the fix develops, rather than silence until release.

Fixes ship in the next release of the supported line. If a problem is severe enough to
warrant an out-of-band release, it will get one.

## Scope

**In scope** — anything this repository builds and publishes:

- The processors (`PublishPulsar`, `ConsumePulsar`, `PublishPulsarRecord`, `ConsumePulsarRecord`)
- The Pulsar client controller service, including credential handling and the client cache
- The published NAR artifacts
- The `ghcr.io/<owner>/nifi` container image, insofar as the problem is in what this
  repository adds on top of the base image

**Out of scope** — please report these upstream, where they can actually be fixed:

- Apache NiFi itself → [security@apache.org](mailto:security@apache.org) and the
  [NiFi security page](https://nifi.apache.org/security.html)
- Apache Pulsar and its Java client → [security@apache.org](mailto:security@apache.org)
- Vulnerabilities in the `apache/nifi` base image that originate upstream. Report them to
  Apache. Telling us anyway is still useful — we may be able to move the base image pin.
- Findings from an automated scanner with no demonstrated impact on this bundle. A CVE
  identifier in a transitive dependency is a starting point for a report, not a report.

## Disclosure

We follow coordinated disclosure. Once a fix is available, the issue is published as a
[GitHub Security Advisory](../../security/advisories) on this repository, and a CVE is
requested where one is warranted. Reporters are credited by name unless they ask not to be.

We will not take action against anyone who reports a vulnerability in good faith through
the channel above, and we ask the same in return: no accessing data that is not yours, no
degrading anyone's service, and no public disclosure before a fix is available.

## Automated scanning

This repository scans itself on a schedule, independently of anything reported:

- **Dependabot** — weekly Maven, GitHub Actions, and base-image updates, grouped and
  auto-merged on green CI for patch and minor bumps (`.github/dependabot.yml`)
- **Trivy** — weekly scan of the dependency tree and the built container image
  (`.github/workflows/security-scan.yml`)
- **CodeQL** — static analysis of this repository's own Java source
  (`.github/workflows/codeql.yml`)

Accepted findings are recorded with a reason in [`.trivyignore`](.trivyignore) rather than
suppressed silently.
