package chat

import "github.com/carolsimone/continuo/agent-chat/domain"

// estimateTokens approximates token count as bytes/4 — close enough for a
// trimming heuristic (the providers enforce the real limit).
func estimateTokens(m domain.Message) int {
	return len(m.Content)/4 + 4
}

// window returns the most recent messages whose estimated tokens fit budget.
// The returned slice:
//   - is never empty when len(msgs) > 0;
//   - always begins on a domain.RoleUser message.
//
// Algorithm:
//  1. Compute start from the tail (greedy token fit).
//  2. Advance start past any leading non-user messages (RoleToolCall, RoleToolResult, RoleAssistant).
//  3. If no user message exists in the budgeted range, fall back to the last
//     RoleUser message in the whole slice.
//  4. If the slice contains no user message at all, return the last message
//     (best-effort non-empty result).
func window(msgs []domain.Message, budgetTokens int) []domain.Message {
	if len(msgs) == 0 {
		return msgs
	}

	// Step 1: greedy token fit from the tail.
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

	// Step 2: advance past any leading non-user messages.
	for start < len(msgs) && msgs[start].Role != domain.RoleUser {
		start++
	}

	// Step 3: if no user message in the budgeted range, fall back to the last
	// RoleUser message in the whole slice.
	if start >= len(msgs) {
		for i := len(msgs) - 1; i >= 0; i-- {
			if msgs[i].Role == domain.RoleUser {
				return msgs[i:]
			}
		}
		// Step 4: no user message at all — return the last message (best-effort).
		return msgs[len(msgs)-1:]
	}

	return msgs[start:]
}
