package bolster

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound occurs when a specific item could not be found.
	ErrNotFound = errors.New("item not found")
	// ErrBadTransaction occurs when a write-action is aborted early because of a faulty transaction.
	ErrBadTransaction = errors.New("abort early: previous error causes transaction rollback")
)

// Error combines error with context information.
type Error struct {
	Action     txAction
	structType structType
	Err        error
}

// IsNotFound returns true if the inner error is ErrNotFound.
func (e Error) IsNotFound() bool {
	return e.Err == ErrNotFound
}

// IsBadTransaction returns true if the inner error is ErrBadTransaction.
func (e Error) IsBadTransaction() bool {
	return e.Err == ErrBadTransaction
}

func newErrorFactory(a txAction, st ...structType) Error {
	e := Error{Action: a}
	if len(st) > 0 {
		e.structType = st[0]
	}
	return e
}

func (e Error) with(err error) error {
	if err == nil {
		return nil
	}
	e.Err = err
	return e
}

// Error implements the built-in error interface.
//
// It combines the action, type info and inner error.
func (e Error) Error() string {
	if e.structType.FullName == nil {
		return fmt.Sprintf("%s: %s", e.Action, e.Err)
	}
	return fmt.Sprintf("%s: %s: %s", e.Action, e.structType, e.Err)
}
