package prompt

import "strings"

// Revise builds the request that asks the model to correct its own answer: the
// same system prompt and the same forced tool as req, and a user message that
// is req's user text followed by the answer the model gave (previousAnswerJSON)
// and what the checks found wrong with it (findings, one line each). req is not
// changed.
func Revise(req ProposeRequest, previousAnswerJSON string, findings []string) ProposeRequest {
	var u strings.Builder
	u.WriteString(req.User)
	u.WriteString("\n\n## Your previous answer\n```json\n")
	u.WriteString(previousAnswerJSON)
	u.WriteString("\n```\n\n## What the checks found\n")
	for _, f := range findings {
		u.WriteString("- ")
		u.WriteString(f)
		u.WriteString("\n")
	}
	u.WriteString("\nCorrect every finding and call the tool again with your complete answer.")

	req.User = u.String()
	return req
}
