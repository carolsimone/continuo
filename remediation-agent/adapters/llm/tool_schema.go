package llm

import "github.com/carolsimone/continuo/remediation-agent/domain/prompt"

// objectSchema is the JSON-Schema object describing a forced tool's
// parameters. Anthropic calls it input_schema and OpenAI calls it parameters,
// but the document itself is identical, so both adapters marshal this one type.
type objectSchema struct {
	Type       string               `json:"type"`
	Properties map[string]paramProp `json:"properties"`
	Required   []string             `json:"required"`
}

// paramProp is one property of a JSON-Schema object. Items is set only for an
// array property and describes what a single element looks like; it is omitted
// for every scalar property.
type paramProp struct {
	Type        string        `json:"type"`
	Description string        `json:"description,omitempty"`
	Items       *objectSchema `json:"items,omitempty"`
}

// buildToolSchema renders a prompt's tool parameters as the JSON-Schema object
// both provider wire formats embed. An array parameter's Items become an
// "items" subschema of type object whose properties are all required, so the
// model is told the exact shape of an element instead of inferring it from the
// parameter description.
func buildToolSchema(params []prompt.ToolParam) objectSchema {
	props := make(map[string]paramProp, len(params))
	required := make([]string, 0, len(params))
	for _, param := range params {
		prop := paramProp{Type: param.Type, Description: param.Description}
		if len(param.Items) > 0 {
			items := buildToolSchema(param.Items)
			// Every property of an element is required: an element missing
			// either half of a path/content pair is not a usable answer.
			items.Required = propertyNames(param.Items)
			prop.Items = &items
		}
		props[param.Name] = prop
		if param.Required {
			required = append(required, param.Name)
		}
	}
	return objectSchema{Type: "object", Properties: props, Required: required}
}

// propertyNames lists the parameter names in declaration order, used as an
// element subschema's required list.
func propertyNames(params []prompt.ToolParam) []string {
	names := make([]string, 0, len(params))
	for _, p := range params {
		names = append(names, p.Name)
	}
	return names
}
