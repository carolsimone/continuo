package fix

// TryBefore reports whether the row of try aTry of round aRound was written
// before the row of try bTry of round bRound: an earlier round, or an earlier
// try of the same round. A round's hand-over row (HandOverTry) is written once
// its tries are spent, so it comes after every try of its round.
func TryBefore(aRound, aTry, bRound, bTry int) bool {
	if aRound != bRound {
		return aRound < bRound
	}
	return tryRank(aTry) < tryRank(bTry)
}

// tryRank places the hand-over row after every numbered try of its round.
func tryRank(try int) int {
	if try == HandOverTry {
		return int(^uint(0) >> 1)
	}
	return try
}
