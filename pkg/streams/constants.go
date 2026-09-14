package streams

// StreamMaxLen is the upper bound every publisher in this repository applies to
// its Redis streams with approximate (`~`) trimming, so a stream a consumer
// group lags on cannot grow until it exhausts Redis memory — a stream ACK does
// not delete the entry, so an uncapped busy stream grows without limit. It is
// 10000, applied independently by each publisher to its own streams. Approximate
// trimming can drop the oldest entries before a lagging group reads them; that is
// the accepted trade-off.
const StreamMaxLen = 10000
