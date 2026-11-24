package ports

// ModelReferences is what a checkout of a dbt project says about one of its
// models, for a fix that renames it: the files that name it, the names the
// project's models already use, and whether the model and the project's other
// models declare what their table is (continuo.entity or continuo.grain).
type ModelReferences struct {
	// Files maps the repository-relative path of each file that names the
	// model to its content: a properties file whose models list has an entry
	// of that name, and a SQL file that refs it. The model's own SQL file is
	// not among them.
	Files map[string]string
	// Order lists the paths of Files, sorted.
	Order []string
	// ModelNames holds every model name the project uses: the base name of
	// each .sql and .py file under it and the name of each models entry of its
	// properties files.
	ModelNames map[string]bool
	// DeclaresKind reports whether the model's own properties entry declares
	// continuo.entity or continuo.grain, in its meta or its config.meta.
	DeclaresKind bool
	// OthersDeclareKind reports whether another model of the project does.
	OthersDeclareKind bool
}

// ModelReferenceFinder reads a dbt project inside a repository checkout.
type ModelReferenceFinder interface {
	// Find walks the dbt project at projectDir, a repository-relative
	// directory under rootDir (a checkout as RepoArchive.Fetch returns it),
	// and reports what names model there. A project directory that does not
	// exist is an error.
	Find(rootDir, projectDir, model string) (ModelReferences, error)
}
