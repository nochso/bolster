package errlist

import (
	"bytes"
	"fmt"
)

// Errors is a list of errors.
type Errors []error

// New returns a list of errors optionally populated with the given errors.
func New(errors ...error) Errors {
	return Errors(errors)
}

// Append adds a non-nil error to the list and returns it.
func (e Errors) Append(err error) Errors {
	if err == nil {
		return e
	}
	return append(e, err)
}

// ErrorOrNil returns the error list if it contains at least one error.
// Otherwise nil is returned.
func (e Errors) ErrorOrNil() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

// HasError returns true if the list contains at least one error.
func (e Errors) HasError() bool {
	return len(e) > 0
}

// Error implements the error interface.
// A single error is formatted as usual.
// Multiple errors are formatted per line with a summary of the error count.
func (e Errors) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "%d errors occurred:\n", len(e))
	for _, err := range e {
		fmt.Fprintf(buf, "* %s\n", err.Error())
	}
	return buf.String()
}
