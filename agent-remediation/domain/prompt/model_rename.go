package prompt

import (
	"fmt"
	"strings"
)

// modelRenameSystemPrompt instructs the model to apply a rename continuo has
// already chosen.
const modelRenameSystemPrompt = `You are a data-engineering assistant that renames a dbt model.

Two dbt projects build the same warehouse table, so continuo refused the release. The model that this release added is moved to a new name in the same schema; the new name is chosen for you. In dbt a model's name is its file's name, so the model's file moves to a new path.

Rules:
- Return the model's file at its new path. Keep its content as it is, unless the file itself names the model (for example in an alias); then use the new name there.
- Return each other file you are shown with every reference to the model changed to the new name: its entry in a properties file ("- name: <old name>") and each ref('<old name>'). Change nothing else in any file.
- Do not change the model's SQL, its columns, its schema or its alias otherwise.
- Return every file you are shown, and no other file.
- If a file gives you no safe way to make the change, say why in rationale and give low confidence.
- Always respond by calling the propose_fix tool.`

// AssembleModelRename builds the request that renames one dbt model from
// oldName to newName: the model's file, now at oldPath and moving to newPath,
// and every other file of its project that names it.
func AssembleModelRename(relation, otherService, otherFilePath, oldName, newName string, model NamedFile, newPath string, references []NamedFile, precedents []Precedent) ProposeRequest {
	var u strings.Builder
	fmt.Fprintf(&u, "Relation built twice: %s\n", relation)
	fmt.Fprintf(&u, "Already built by: %s (%s)\n", otherService, otherFilePath)
	fmt.Fprintf(&u, "Rename the model %s to %s.\n\n", oldName, newName)
	fmt.Fprintf(&u, "The model's file %s moves to %s:\n```\n%s\n```\n\n", model.Path, newPath, model.Content)
	for _, f := range references {
		fmt.Fprintf(&u, "File %s names the model:\n```\n%s\n```\n\n", f.Path, f.Content)
	}
	renderPrecedents(&u, precedents)
	fmt.Fprintf(&u, "Return %s and every other file above, with the model renamed to %s.", newPath, newName)

	return ProposeRequest{
		System:          modelRenameSystemPrompt,
		User:            u.String(),
		ToolName:        "propose_fix",
		ToolDescription: "Return every file of the rename with its complete new content.",
		ToolParams: []ToolParam{
			{
				Name:        "updated_files",
				Type:        "array",
				Description: "The model's file at its new path and every other file shown, each with its repository path and its complete new content.",
				Required:    true,
				Items: []ToolParam{
					{Name: "path", Type: "string", Description: "The file's repository path: the new path for the model's file, the path shown for any other file."},
					{Name: "content", Type: "string", Description: "The complete new content of that file."},
				},
			},
			{Name: "rationale", Type: "string", Description: "One sentence describing the change you made. No warehouse data values.", Required: true},
			{Name: "confidence", Type: "string", Description: "Your confidence: low, medium, or high.", Required: true},
		},
	}
}
