package utils

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
)

var debugMode = false

var (
	ErrEOF = errors.New("ErrEOF: End of file")
)

// Check logs fatal if err != nil.
func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", Wrap(err, ""))
	}
}

// Check2 acts as convenience wrapper around Check, using the 2nd argument as error.
func Check2(_ interface{}, err error) {
	Check(err)
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

// Wrap wraps errors from external lib.
func Wrap(err error, msg string) error {
	if !debugMode {
		if err == nil {
			return nil
		}
		return fmt.Errorf("%s err: %+v", msg, err)
	}
	return errors.Wrap(err, msg)
}

// Wrapf is Wrap with extra info.
func Wrapf(err error, format string, args ...interface{}) error {
	if !debugMode {
		if err == nil {
			return nil
		}
		return fmt.Errorf(format+" error: %+v", append(args, err)...)
	}
	return errors.Wrapf(err, format, args...)
}

func CombineErrors(one, other error) error {
	if one != nil && other != nil {
		return fmt.Errorf("%v; %v", one, other)
	}
	if one != nil && other == nil {
		return fmt.Errorf("%v", one)
	}
	if one == nil && other != nil {
		return fmt.Errorf("%v", other)
	}
	return nil
}
