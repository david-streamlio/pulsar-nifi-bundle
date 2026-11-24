#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM apache/nifi:2.12.0

#############################################################################
# Apply Debian security updates
#############################################################################
# apache/nifi:<version> is built once per NiFi release and never rebuilt, so its
# Debian packages only age: a few weeks in, the scan finds HIGH and CRITICAL CVEs
# that Debian has long since fixed. Upgrading here picks those fixes up on every
# build - the weekly scan and each release - without moving off the NiFi runtime
# the NARs are compiled against. It updates installed packages only; nothing new
# is installed.
USER root
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
       -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER nifi

#############################################################################
# Add the custom NAR files
#############################################################################

COPY --chown=nifi:nifi maven/modules/maven-assembly-plugin/nifi-pulsar-*.nar /opt/nifi/nifi-current/lib/

#############################################################################
# Add resource files for testing
#############################################################################
COPY --chown=nifi:nifi resources /resources
