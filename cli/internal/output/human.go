package output

import (
	"fmt"
	"io"
)

// HumanSuccess writes a one-line confirmation to the given stderr writer.
func HumanSuccess(stderr io.Writer, line string) error {
	_, err := fmt.Fprintln(stderr, line)
	return err
}

// HumanError writes a single line "Error [code]: message" to the given stderr writer.
func HumanError(stderr io.Writer, e CLIError) error {
	_, err := fmt.Fprintf(stderr, "Error [%s]: %s\n", e.Code, e.Message)
	return err
}
