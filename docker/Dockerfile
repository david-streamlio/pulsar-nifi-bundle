FROM apache/nifi:1.14.0

COPY --chown=nifi:nifi lib/*.nar /opt/nifi/nifi-current/lib/

COPY --chown=nifi:nifi creds/*.json /tmp
