# Try it locally, with real data projects

Run Continuo end to end on your laptop in about an hour, with four real data
projects — three dbt, one python. You will watch the platform discover
dependencies that cross project boundaries, validate a change against production
before it promotes, and refuse a change that would break another team's model.
Everything runs on your machine; nothing is deployed anywhere, and no step needs
a cloud account. The walkthrough comes in two parts:

1. [Instantiate the Continuo platform](instantiate-continuo.md) — get an empty
   Continuo running locally, in about ten minutes.
2. [Run dbt and Python projects in Continuo](run-projects-in-continuo.md) — put
   the four projects on it and drive the whole release, validation, and
   remediation loop.
