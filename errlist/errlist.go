package errlist

import (
	"bytes"
	"fmt"
)

// Errors is a list of errors.
type Errors struct {
	errs []error
}

// New returns a list of errors optionally populated with the given errors.
func New(errors ...error) *Errors {
	return &Errors{errs: errors}
}

// Append adds a non-nil error to the list and returns the error as-is.
func (e *Errors) Append(err error) error {
	if err == nil {
		return err
	}
	if list, ok := err.(*Errors); ok {
		e.errs = append(e.errs, list.errs...)
	} else {
		e.errs = append(e.errs, err)
	}
	return err
}

// Last returns the latest error.
func (e *Errors) Last() error {
	if e.HasError() {
		return e.errs[len(e.errs)-1]
	}
	return nil
}

// ErrorOrNil returns the error list if it contains more than one error.
// If there's only one error, it is returned as-is.
// If there are no errors, nil is returned.
func (e *Errors) ErrorOrNil() error {
	switch len(e.errs) {
	case 0:
		return nil
	case 1:
		return e.errs[0]
	default:
		return e
	}
}

// HasError returns true if the list contains at least one error.
func (e *Errors) HasError() bool {
	return len(e.errs) > 0
}

// Error implements the error interface.
// A single error is formatted as usual.
// Multiple errors are formatted per line with a summary of the error count.
func (e *Errors) Error() string {
	if len(e.errs) == 1 {
		return e.errs[0].Error()
	}
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "%d errors occurred:\n", len(e.errs))
	for _, err := range e.errs {
		fmt.Fprintf(buf, "* %s\n", err.Error())
	}
	return buf.String()
}
