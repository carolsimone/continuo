# dead-letter-controller

dead-letter-controller keeps every message a component gave up on, so an operator can read why and send it again. The shared words (dead letter, replay horizon, component) are in the root `GLOSSARY.md`.

## Language

**Redrive**:
Sending a dead letter again to the component that gave up on it, once its cause is fixed. It records who sent it and why.
_Avoid_: replay, retry, resend

**Dead-letter kind**:
Why a component gave up on a message: a permanent error no retry can fix, a temporary error that outlasted every retry, or a message removed from its stream before it was finished.
_Avoid_: failure type, error class
