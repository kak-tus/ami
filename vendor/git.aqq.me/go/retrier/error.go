package retrier

// Error type represents error instance.
type Error struct {
	err   error
	fatal bool
}

// NewError method creates new regular error instance.
func NewError(err error, fatal bool) *Error {
	return &Error{
		err:   err,
		fatal: fatal,
	}
}

func (e *Error) Error() string {
	return e.err.Error()
}

// IsFatal method checks if the error is fatal.
func (e *Error) IsFatal() bool {
	return e.fatal
}
