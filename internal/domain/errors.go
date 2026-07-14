package domain

import (
	"errors"
	"fmt"
)

var (
	ErrJobNotFound         = errors.New("job not found")
	ErrInvalidJobTransition = errors.New("invalid job status transition")
)

type InvalidJobTransitionError struct {
	JobID string
	From  string
	To    string
}

func (e *InvalidJobTransitionError) Error() string {
	return fmt.Sprintf(
		"job %q cannot transition from %q to %q",
		e.JobID,
		e.From,
		e.To,
	)
}

func (e *InvalidJobTransitionError) Unwrap() error {
	return ErrInvalidJobTransition
}