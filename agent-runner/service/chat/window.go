package chat

import "github.com/carolsimone/continuo/agent-runner/domain"

// estimateTokens approximates token count as bytes/4 — close enough for a
// trimming heuristic (the providers enforce the real limit).
func estimateTokens(m domain.Message) int {
	return len(m.Content)/4 + 4
}

// window returns the most recent messages whose estimated tokens fit budget.
// It never starts on a tool_result (its tool_call would be missing, which
// providers reject): orphaned results at the head are dropped too.
func window(msgs []domain.Message, budgetTokens int) []domain.Message {
	total := 0
	start := len(msgs)
	for i := len(msgs) - 1; i >= 0; i-- {
		cost := estimateTokens(msgs[i])
		if total+cost > budgetTokens {
			break
		}
		total += cost
		start = i
	}
	for start < len(msgs) && msgs[start].Role == domain.RoleToolResult {
		start++
	}
	return msgs[start:]
}
