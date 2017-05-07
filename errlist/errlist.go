package errlist

import (
	"bytes"
	"fmt"
)

type Errors []error

func New(errors ...error) Errors {
	return Errors(errors)
}

func (e Errors) Append(err error) Errors {
	if err == nil {
		return e
	}
	return append(e, err)
}

func (e Errors) ErrorOrNil() error {
	if len(e) == 0 {
		return nil
	}
	return e
}

func (e Errors) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, "%d errors occured:\n", len(e))
	for _, err := range e {
		fmt.Fprintf(buf, "* %s\n", err.Error())
	}
	return buf.String()
}
