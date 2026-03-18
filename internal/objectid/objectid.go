package objectid

import (
	"encoding/hex"

	bson "go.mongodb.org/mongo-driver/v2/bson"
)

// Parse parses the given string and returns
// a bson.ObjectID and true if the given value is valid,
// otherwise it will return an empty bson.ObjectID and false.
//
// This is copy of the bson.ObjectIDFromHex behavior, but
// prevents doing double decode in the classic procedure:
// if bson.IsObjectIDHex(x) { bson.ObjectIDFromHex(x) }.
func Parse(s string) (bson.ObjectID, bool) {

	if len(s) != 24 {
		return bson.NilObjectID, false
	}

	d, err := hex.DecodeString(s)
	if err != nil || len(d) != 12 {
		return bson.NilObjectID, false
	}

	var oid bson.ObjectID
	copy(oid[:], d)
	return oid, true
}
