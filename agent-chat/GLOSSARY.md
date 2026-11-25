# agent-chat

agent-chat is the conversation in the ui where a person asks a model about continuo and has it act, through the `continuo` command line, with the person's approval for anything that changes something.

## Language

**Thread**:
One conversation between a person and the model, kept with every message and tool call in it.
_Avoid_: chat, session, conversation id

**Tool**:
One `continuo` command the model may call, with its arguments and the flags marked for the chat.
_Avoid_: function, skill, plugin

**Tool call**:
One use of a tool the model asks for in a thread, and the answer it got back.
_Avoid_: function call, invocation

**Mutating tool call**:
A tool call that changes something, such as starting or cancelling a run. It waits for the person's approval before it runs, and runs at most once however often it is sent.
_Avoid_: write call, action

**Pending action**:
A mutating tool call waiting for the person to approve or deny it. Left alone too long, it expires.
_Avoid_: approval request, confirmation
