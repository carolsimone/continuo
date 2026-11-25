# remediation

remediation sorts the failures of a rejected release: for each failing node it decides whether a fix is worth trying, then sends one heal trigger per rejected release and round. The shared words (rejection, reject reason, heal trigger, round, unfixable node, error signature, hand-over) are in the root `GLOSSARY.md`.

## Language

**Classification**:
The decision, for one failing node, whether a fix is worth trying or the node is unfixable. It is made once per node and round, and kept.
_Avoid_: triage, categorisation, verdict

**Key error line**:
The one line of a failure's output that says what actually failed. A node handed over as unfixable carries it as its reason.
_Avoid_: error message, log excerpt

**Rule**:
A phrase in a failure's key error line that decides its classification, such as a refused connection.
_Avoid_: pattern, matcher, heuristic

**Rule family**:
What kind of failure a rule recognises: infrastructure, parse, logic, test, or unknown when no rule matched.
_Avoid_: category, failure type

**Infrastructure failure**:
A failure caused by the platform the code ran on, not by the code, such as a lost connection or a pod out of memory. It is always unfixable.
_Avoid_: system error, environment failure
