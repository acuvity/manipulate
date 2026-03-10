package manipmongo

import "errors"

var (
	// ErrMongoAPIRequiresMongoManipulator indicates that the called helper only
	// works with the official mongo-driver-backed manipulator.
	ErrMongoAPIRequiresMongoManipulator = errors.New("mongo driver api requires a mongo manipulator")
)
