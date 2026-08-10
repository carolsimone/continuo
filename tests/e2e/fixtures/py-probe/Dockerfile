# The e2e fixture domain image, built exactly the way the runtime repository's
# template prescribes: the engine-matched runtime base plus this service's
# contracts and scripts. One image serves both of the service's nodes — the
# harness selects between them from NODE_ID at run time.
FROM ghcr.io/carolsimone/continuo-python-runtime:v0.2.1-postgres
COPY contracts/ /app/contracts/
COPY scripts/ /app/scripts/
