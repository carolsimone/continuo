# The e2e fixture domain image, built exactly the way the runtime repository's
# template prescribes: the engine-matched runtime base plus this service's
# contracts and scripts. One image serves both of the service's nodes — the
# harness selects between them from NODE_ID at run time.
FROM ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.4.0
COPY contracts/ /app/contracts/
COPY scripts/ /app/scripts/
# The runtime base already declares USER 65532:65532 — a frozen cross-repo
# contract with executor-controller, which hard-codes that uid in its validation
# pod securityContext. Nothing to set here: the harness only reads its baked
# contracts and scripts and writes solely to the warehouse, so no ownership
# changes are needed on top of the base's default user.
