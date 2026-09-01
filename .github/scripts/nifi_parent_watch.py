#!/usr/bin/env python3
"""Reports what the pinned NiFi parent POM is holding back (#206).

The parent `org.apache.nifi:nifi-extension-bundles` is pinned, and Dependabot is configured
to ignore minor and major updates to it, because a newer parent bans JUnit 4 and forces a
JUnit 4 -> 5 migration (see #107). The reasoning is sound; the consequence is that the pin is
the one input to this build that nothing reports on, so "we deferred this" and "we forgot
this" became indistinguishable from the outside.

What this reports is deliberately NOT "are there new NiFi CVEs". This bundle takes its NiFi
API dependencies through the `nifi.version` property, which is kept current independently, so
NiFi's own advisories are largely answered elsewhere. What the parent actually governs is
dependency MANAGEMENT - the versions our modules inherit without naming, such as the JUnit
BOM. So the question worth answering weekly is: which managed dependency versions would
change if the pin moved, and do any of those carry known security fixes.

Prints a Markdown report to stdout. Exits non-zero only on error, never because findings
exist: this is a report, not a gate.
"""

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
CENTRAL = "https://repo1.maven.org/maven2"
MAX_PARENT_DEPTH = 6
TIMEOUT = 60


def text_of(node, path):
    found = node.find(path, NS)
    return found.text.strip() if found is not None and found.text else None


def fetch(url):
    with urllib.request.urlopen(url, timeout=TIMEOUT) as response:
        return response.read()


def pinned_parent(pom_path):
    """The parent this repository actually pins, read as XML rather than grepped."""
    parent = ET.parse(pom_path).getroot().find("m:parent", NS)
    if parent is None:
        raise SystemExit(f"{pom_path} has no <parent>; nothing to watch")

    return (text_of(parent, "m:groupId"), text_of(parent, "m:artifactId"),
            text_of(parent, "m:version"))


def latest_release(group, artifact):
    """The newest released version, from Maven Central's metadata.

    maven-metadata.xml carries no XML namespace, unlike a POM, so these paths are
    deliberately namespace-free. Getting that wrong made this report claim "up to date"
    while silently finding nothing - the exact failure this watcher exists to prevent, so
    an undeterminable version is raised rather than reported as no news.
    """
    path = f"{CENTRAL}/{group.replace('.', '/')}/{artifact}/maven-metadata.xml"
    root = ET.fromstring(fetch(path))

    release = (root.findtext("versioning/release") or root.findtext("versioning/latest") or "").strip()
    versions = [v.text.strip() for v in root.findall("versioning/versions/version") if v.text]

    if not release or not versions:
        raise SystemExit(f"could not determine the latest release of {group}:{artifact} from {path}")

    return release, versions


def version_key(version):
    """Sorts 2.10.0 above 2.9.0, which a string comparison does not."""
    return tuple(int(part) if part.isdigit() else 0
                 for part in re.split(r"[.\-]", version)[:4])


def managed_versions(group, artifact, version):
    """Managed dependencies from this POM and every parent above it, resolving properties.

    A child's entry wins over a parent's, which is how Maven itself resolves them.
    """
    collected, properties, depth = {}, {}, 0

    while group and artifact and version and depth < MAX_PARENT_DEPTH:
        url = f"{CENTRAL}/{group.replace('.', '/')}/{artifact}/{version}/{artifact}-{version}.pom"

        try:
            root = ET.fromstring(fetch(url))
        except (urllib.error.URLError, ET.ParseError) as error:
            print(f"::warning::could not read {artifact} {version}: {error}", file=sys.stderr)
            break

        props = root.find("m:properties", NS)
        for prop in (props if props is not None else []):
            key = prop.tag.split("}")[-1]
            properties.setdefault(key, (prop.text or "").strip())

        managed = root.find("m:dependencyManagement/m:dependencies", NS)
        for dependency in (managed if managed is not None else []):
            coordinate = f"{text_of(dependency, 'm:groupId')}:{text_of(dependency, 'm:artifactId')}"
            collected.setdefault(coordinate, text_of(dependency, "m:version") or "(inherited)")

        parent = root.find("m:parent", NS)
        if parent is None:
            break

        group = text_of(parent, "m:groupId")
        artifact = text_of(parent, "m:artifactId")
        version = text_of(parent, "m:version")
        depth += 1

    return {coordinate: resolve(value, properties) for coordinate, value in collected.items()}


def resolve(value, properties, depth=0):
    """Expands ${...} against the properties gathered from the POM chain."""
    if not value or "${" not in value or depth > 5:
        return value

    def swap(match):
        return properties.get(match.group(1), match.group(0))

    expanded = re.sub(r"\$\{([^}]+)\}", swap, value)
    return expanded if expanded == value else resolve(expanded, properties, depth + 1)


def advisories_for(coordinate, pinned, available):
    """Advisories for one coordinate that the pinned version has and a bump would fix.

    An advisory whose fix landed at or below the pinned version is already carried and is not
    news; one whose fix lands above the available version would not be fixed by this bump
    either. Only what sits between the two is a reason to move.

    Version comparison is numeric-segment only, so an exotic scheme may misplace an advisory -
    the report says as much rather than implying precision it does not have. Any failure
    yields nothing: an advisory lookup must never fail the report.
    """
    try:
        result = subprocess.run(
            ["gh", "api", f"/advisories?ecosystem=maven&affects={coordinate}&per_page=50"],
            capture_output=True, text=True, timeout=60, check=False)

        if result.returncode != 0:
            return []

        relevant = []

        for item in json.loads(result.stdout or "[]"):
            patched = [
                vulnerability.get("first_patched_version")
                for vulnerability in item.get("vulnerabilities") or []
                if vulnerability.get("package", {}).get("name") == coordinate
                and vulnerability.get("first_patched_version")]

            if any(version_key(pinned) < version_key(fix) <= version_key(available)
                   for fix in patched):
                relevant.append({"id": item.get("ghsa_id"),
                                 "severity": (item.get("severity") or "unknown").lower()})

        return relevant
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError, ValueError):
        return []


def main():
    pom = os.environ.get("POM_PATH", "pom.xml")
    group, artifact, pinned = pinned_parent(pom)
    release, all_versions = latest_release(group, artifact)

    print(f"# Pinned NiFi parent: `{artifact}` {pinned}\n")

    if not release or version_key(release) <= version_key(pinned):
        print(f"Up to date with the latest release on Maven Central (`{release}`). Nothing to report.")
        return 0

    behind = [v for v in all_versions
              if version_key(pinned) < version_key(v) <= version_key(release)]

    print(f"**{len(behind)} release(s) behind.** Pinned at `{pinned}`; latest is `{release}`.\n")
    print(f"Intervening releases: {', '.join(f'`{v}`' for v in behind)}\n")

    old = managed_versions(group, artifact, pinned)
    new = managed_versions(group, artifact, release)

    changed = sorted((c, old[c], new[c]) for c in set(old) & set(new) if old[c] != new[c])
    added = sorted(set(new) - set(old))
    removed = sorted(set(old) - set(new))

    print("## What this pin holds back\n")
    print("These are versions our modules inherit from the parent without naming them, so the "
          "pin decides them. This is what the parent actually governs - NiFi's own API "
          "dependencies come from the `nifi.version` property, which moves independently and "
          "is current.\n")

    urgent, minor, quiet = [], [], []

    for coordinate, was, now in changed:
        found = advisories_for(coordinate, was, now)
        severities = {a["severity"] for a in found}

        if severities & {"high", "critical"}:
            urgent.append((coordinate, was, now, found))
        elif found:
            minor.append((coordinate, was, now, found))
        else:
            quiet.append((coordinate, was, now))

    def table(rows):
        print("| Dependency | Pinned | Available | Advisories fixed by moving |")
        print("|---|---|---|---|")
        for coordinate, was, now, found in rows:
            listed = ", ".join(f"{a['id']} ({a['severity']})" for a in found if a.get("id"))
            print(f"| `{coordinate}` | `{was}` | `{now}` | {listed or '-'} |")
        print()

    if urgent:
        print(f"### {len(urgent)} carrying HIGH or CRITICAL fixes - this is the part that demands attention\n")
        table(urgent)
    else:
        print("### Nothing carrying a high or critical fix\n")
        print("No dependency this pin holds back has an unfixed high or critical advisory that "
              "moving the parent would resolve.\n")

    if minor:
        print(f"<details><summary>{len(minor)} carrying lower-severity fixes</summary>\n")
        table(minor)
        print("</details>\n")

    if quiet:
        print(f"<details><summary>{len(quiet)} version changes with no known advisory</summary>\n")
        for coordinate, was, now in quiet:
            print(f"- `{coordinate}`: `{was}` -> `{now}`")
        print("\n</details>\n")

    for label, entries in (("added", added), ("no longer managed", removed)):
        if entries:
            print(f"<details><summary>{len(entries)} dependency(ies) {label}</summary>\n")
            for coordinate in entries:
                print(f"- `{coordinate}`")
            print("\n</details>\n")

    print("## How to read this\n")
    print("A version change alone is not a reason to act; the first table is. Each advisory "
          "listed there is unfixed at the pinned version and fixed at or below the available "
          "one, so moving the parent would carry the fix.\n")
    print("Advisories already fixed in the pinned version are excluded, as are those needing a "
          "version newer than the parent offers. Version comparison is numeric-segment only, so "
          "an unusual versioning scheme could misplace an entry - check before acting on one.\n")
    print("The pin is deliberate (#107): a newer parent bans JUnit 4 and forces a JUnit 4 to 5 "
          "migration. This report exists so that deferral stays a decision rather than becoming "
          "an oversight. Tracked by #206.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
