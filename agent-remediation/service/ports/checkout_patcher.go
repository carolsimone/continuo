package ports

// CheckoutPatcher writes a draft's proposed files over a repository checkout
// on local disk, such as the directory RepoArchive.Fetch returns, so a check
// can run against the repository as the draft would leave it.
type CheckoutPatcher interface {
	// Apply writes each file into the checkout at root, creating the
	// directories it needs, and returns what each path held beforehand, keyed
	// by the path as the file names it. Every original is read before anything
	// is written, so the originals describe the repository's own content; a
	// path the checkout does not have reads as empty. A path that is empty,
	// absolute or resolves outside the checkout is refused before anything is
	// read or written. An error can leave some files written, so the caller
	// discards the checkout.
	Apply(root string, files []ProposedFile) (originals map[string]string, err error)
}
