package remediation

// Draft is the model's proposed fix for one cluster: the file edits, the node
// it names as the one to edit, and the model's own account of the change.
type Draft struct {
	Edits []Edit
	// NamedTarget is the node id the model named as the one to edit.
	NamedTarget string
	// TargetShown reports whether the named target was one of the nodes the
	// prompt showed the model.
	TargetShown   bool
	Rationale     string
	Confidence    string
	Model         string
	SuspectedRoot string
	// CandidateFixSQLURI and CandidateFixDiffURI locate the fix's stored SQL
	// and diff for a dbt-model fix.
	CandidateFixSQLURI  string
	CandidateFixDiffURI string
	// SourceResolved reports whether the fix's source was resolved from the
	// repository or the code bundle.
	SourceResolved bool
}

// Edit is one file change of a draft. Original is the file as read, Content the
// file after the edit. ContentKey and DiffKey name the stored content and diff
// of the edit, ContentURI and DiffURI locate them. RenamedFrom is set on an edit
// that moves a file: the file at RenamedFrom (whose content is Original) is
// deleted and Content is written at Path; it is empty for an edit that changes
// Path in place.
type Edit struct {
	Path         string
	RenamedFrom  string
	Original     string
	Content      string
	TargetNodeID string
	ContentKey   string
	DiffKey      string
	ContentURI   string
	DiffURI      string
}
