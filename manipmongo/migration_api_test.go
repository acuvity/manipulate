package manipmongo

import (
	"errors"
	"strings"
	"testing"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate/maniptest"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestCanonicalHelpersRejectNonMongoManipulator(t *testing.T) {
	identity := elemental.MakeIdentity("resource", "resources")
	m := maniptest.NewTestManipulator()

	err := createIndexCanonical(m, identity, mongo.IndexModel{Keys: mongobson.D{{Key: "name", Value: 1}}})
	if !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("expected ErrMongoAPIRequiresMongoManipulator from CreateIndex, got: %v", err)
	}

	err = ensureIndexCanonical(m, identity, mongo.IndexModel{Keys: mongobson.D{{Key: "name", Value: 1}}})
	if !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("expected ErrMongoAPIRequiresMongoManipulator from EnsureIndex, got: %v", err)
	}

	err = createCollectionCanonical(m, identity, mongooptions.CreateCollection())
	if !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("expected ErrMongoAPIRequiresMongoManipulator from CreateCollection, got: %v", err)
	}

	_, closeFn, err := getDatabaseCanonical(m)
	if closeFn != nil {
		closeFn()
	}
	if !errors.Is(err, ErrMongoAPIRequiresMongoManipulator) {
		t.Fatalf("expected ErrMongoAPIRequiresMongoManipulator from GetDatabase, got: %v", err)
	}
}

func TestCanonicalHelpersCreateCollectionAndIndexes(t *testing.T) {
	uri, db := requireMemongo(t)
	m, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	collectionIdentity := elemental.MakeIdentity("resourcecanonicalc", "resourcecanonicalcs")
	indexIdentity := elemental.MakeIdentity("resourcecanonicali", "resourcecanonicalis")
	ensureIdentity := elemental.MakeIdentity("resourcecanonicale", "resourcecanonicales")

	err = createCollectionCanonical(m, collectionIdentity, mongooptions.CreateCollection())
	if err != nil {
		t.Fatalf("create collection failed: %v", err)
	}

	err = createIndexCanonical(m, indexIdentity, mongo.IndexModel{Keys: mongobson.D{{Key: "name", Value: 1}}})
	if err != nil {
		t.Fatalf("create index failed: %v", err)
	}

	err = createCollectionCanonical(m, ensureIdentity, mongooptions.CreateCollection())
	if err != nil {
		t.Fatalf("unable to create ensure collection: %v", err)
	}
	err = ensureIndexCanonical(m, ensureIdentity, mongo.IndexModel{Keys: mongobson.D{{Key: "name", Value: 1}}})
	if err != nil {
		t.Fatalf("ensure index failed: %v", err)
	}
}

func TestNewRejectsNegativeConnectionPoolLimit(t *testing.T) {
	_, err := New("mongodb://127.0.0.1:27017", "db", OptionConnectionPoolLimit(-1))
	if err == nil || !strings.Contains(err.Error(), "invalid connection pool limit") {
		t.Fatalf("expected invalid connection pool limit error, got: %v", err)
	}
}
