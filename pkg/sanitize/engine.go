package sanitize

import (
	"sort"
	"strings"
)

// maxPasses bounds how many times a text is searched again after a pass
// redacted something. Each pass that redacts leaves less plain text or fewer
// markers, so a text settles long before the bound.
const maxPasses = 16

// piece is a stretch of a text: plain text the rules left as it was, or, when
// kind is set, a marker that stands for the text it replaced.
type piece struct {
	text string
	kind kind
}

func fixedMarker(k kind, _ string) string { return "[redacted:" + string(k) + "]" }

// render writes the pieces in order, plain text as it is and each marker as the
// label the caller gives its kind and the text it replaced.
func render(pieces []piece, label func(k kind, value string) string) string {
	if len(pieces) == 1 && pieces[0].kind == "" {
		return pieces[0].text
	}
	var b strings.Builder
	for _, p := range pieces {
		if p.kind == "" {
			b.WriteString(p.text)
		} else {
			b.WriteString(label(p.kind, p.text))
		}
	}
	return b.String()
}

// redactPieces searches s for what the rules redact and replaces it with
// markers, then searches the result again, until a search finds nothing. The
// text is read as it will be the next time it is cleaned, markers and all, so
// cleaning a cleaned text finds nothing, and no part of a value is left
// beside a marker that stands where the rest of it was.
func redactPieces(s string) []piece {
	pieces := []piece{{text: s}}
	for range maxPasses {
		rendered := render(pieces, fixedMarker)
		spans := findSpans(rendered)
		if len(spans) == 0 {
			break
		}
		pieces = replaceSpans(pieces, len(rendered), spans)
	}
	return pieces
}

// replaceSpans cuts the spans, which are positions in the rendered pieces, out
// of the pieces and puts a marker in the place of each. total is the length of
// the rendered pieces. A marker holds the text it replaces, the text of the
// markers it takes in included.
func replaceSpans(pieces []piece, total int, spans []span) []piece {
	var out []piece
	add := func(p piece) {
		if p.kind == "" && len(out) > 0 && out[len(out)-1].kind == "" {
			out[len(out)-1].text += p.text
			return
		}
		out = append(out, p)
	}

	i, off, pos := 0, 0, 0 // the piece, how much of it is cut, and the position in the rendered text
	take := func(upto int) []piece {
		var taken []piece
		for pos < upto && i < len(pieces) {
			p := pieces[i]
			if p.kind != "" {
				taken = append(taken, p)
				pos += len(fixedMarker(p.kind, ""))
				i++
				off = 0
				continue
			}
			if rest := len(p.text) - off; upto-pos >= rest {
				taken = append(taken, piece{text: p.text[off:]})
				pos += rest
				i++
				off = 0
				continue
			}
			n := upto - pos
			taken = append(taken, piece{text: p.text[off : off+n]})
			off += n
			pos += n
		}
		return taken
	}

	for _, sp := range spans {
		for _, p := range take(sp.start) {
			add(p)
		}
		var value strings.Builder
		for _, p := range take(sp.end) {
			value.WriteString(p.text)
		}
		add(piece{text: value.String(), kind: sp.kind})
	}
	for _, p := range take(total) {
		add(p)
	}
	return out
}

// heldSpan is a stretch of the text that is held: a marker the text already
// holds, or a match of a rule.
type heldSpan struct {
	span
	order  int // the index of the rule that found it
	marker bool
	dead   bool // taken in by a wider span
}

// holder tracks which stretches of a text are held.
type holder struct {
	size  int
	owner []int // owner[i] is 1 plus the id of the span that holds byte i, 0 when it is free
	spans []heldSpan
}

func (h *holder) set(start, end, id int) {
	if h.owner == nil {
		h.owner = make([]int, h.size)
	}
	for i := start; i < end; i++ {
		h.owner[i] = id + 1
	}
}

// overlapping returns the ids of the spans that hold a byte of [start, end).
func (h *holder) overlapping(start, end int) []int {
	if h.owner == nil {
		return nil
	}
	var ids []int
	last := 0
	for _, o := range h.owner[start:end] {
		if o != 0 && o != last {
			ids = append(ids, o-1)
		}
		last = o
	}
	return ids
}

func (h *holder) addMarker(start, end int) {
	h.spans = append(h.spans, heldSpan{span: span{start: start, end: end}, marker: true})
	h.set(start, end, len(h.spans)-1)
}

// claim holds a match of the rule at index order, unless it lies inside what is
// held already or only partly overlaps a match of a rule. A match that reaches
// into a marker takes the marker in. A match of a rule that takes in also takes
// in the earlier matches inside it, and the span it makes is of the kind of the
// earliest rule among them.
func (h *holder) claim(c span, order int, takesIn bool) {
	start, end := c.start, c.end
	var absorbed []int
	for {
		changed := false
		absorbed = absorbed[:0]
		for _, id := range h.overlapping(start, end) {
			held := h.spans[id]
			if held.start <= c.start && c.end <= held.end {
				return
			}
			if !held.marker && (!takesIn || held.start < start || held.end > end) {
				return
			}
			if held.start < start {
				start, changed = held.start, true
			}
			if held.end > end {
				end, changed = held.end, true
			}
			absorbed = append(absorbed, id)
		}
		if !changed {
			break
		}
	}

	first := order
	for _, id := range absorbed {
		if held := h.spans[id]; !held.marker {
			first = min(first, held.order)
		}
		h.spans[id].dead = true
	}
	h.spans = append(h.spans, heldSpan{span: span{start, end, rules[first].kind}, order: first})
	h.set(start, end, len(h.spans)-1)
}

// findSpans returns the spans of s to replace, by position and without
// overlap. The markers s holds are held first, so no rule reads one as a key or
// a value, and a match that reaches into one takes it in whole.
func findSpans(s string) []span {
	h := holder{size: len(s)}
	for _, m := range markerAny.FindAllStringIndex(s, -1) {
		h.addMarker(m[0], m[1])
	}
	lower := strings.ToLower(s)
	for order, r := range rules {
		for _, p := range r.patterns {
			for _, c := range p.candidates(r.kind, s, lower) {
				h.claim(c, order, r.takesIn)
			}
		}
	}

	var spans []span
	for _, held := range h.spans {
		if !held.marker && !held.dead {
			spans = append(spans, held.span)
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })
	return spans
}
