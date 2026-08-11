# The e2e fixture domain image, built exactly the way the runtime repository's
# template prescribes: the engine-matched runtime base plus this service's
# contracts and scripts. One image serves both of the service's nodes — the
# harness selects between them from NODE_ID at run time.
FROM ghcr.io/carolsimone/continuo-python-runtime:v0.2.1-postgres
COPY contracts/ /app/contracts/
COPY scripts/ /app/scripts/
# The runtime base declares no non-root user, so the executor leaves the user to
# the image (its pod spec drops capabilities and forbids privilege escalation
# but sets no uid). A domain image should therefore pick its own. The harness
# only reads its baked contracts and scripts and writes solely to the warehouse,
# so an unprivileged uid needs no ownership changes.
USER 65532:65532
