package manipmongo

import (
	"fmt"

	bson "go.mongodb.org/mongo-driver/v2/bson"
)

func cloneBSONValue[T any](in any) (T, error) {
	var out T
	if in == nil {
		return out, nil
	}
	data, err := bson.Marshal(in)
	if err != nil {
		return out, fmt.Errorf("unable to marshal bson value: %w", err)
	}
	if err := bson.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("unable to unmarshal bson value: %w", err)
	}
	return out, nil
}
