package manipmongo

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

var mongoStringIdentity = elemental.MakeIdentity("mongostringitem", "mongostringitems")

type mongoStringIDObject struct {
	ID          string `bson:"_id,omitempty"`
	Name        string `bson:"name"`
	Description string `bson:"description"`
}

func (o *mongoStringIDObject) Identifier() string           { return o.ID }
func (o *mongoStringIDObject) SetIdentifier(id string)      { o.ID = id }
func (o *mongoStringIDObject) Identity() elemental.Identity { return mongoStringIdentity }
func (o *mongoStringIDObject) String() string               { return o.Name }
func (o *mongoStringIDObject) Version() int                 { return 0 }
func (o *mongoStringIDObject) ValueForAttribute(string) any { return nil }

func newMongoStringIDObject(name string) *mongoStringIDObject {
	return &mongoStringIDObject{Name: name, Description: "desc-" + name}
}

func TestMongoStringIDAndCountBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)
	manipulator, err := New(uri, db, OptionForceReadFilter(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	dbObj, err := GetDatabase(manipulator)
	if err != nil {
		t.Fatalf("unable to get mongo db: %v", err)
	}

	seedID := "mongo-string-id"
	_, err = dbObj.Collection(mongoStringIdentity.Name).InsertOne(context.Background(), mongobson.M{
		"_id":         seedID,
		"name":        "mongo-string",
		"description": "mongo desc",
	})
	if err != nil {
		t.Fatalf("unable to seed mongo string-id doc: %v", err)
	}

	obj := newMongoStringIDObject("mongo-string")
	obj.SetIdentifier(seedID)
	if err := manipulator.Retrieve(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals("mongo-string").Done()),
			manipulate.ContextOptionFields([]string{"name"}),
		),
		obj,
	); err != nil {
		t.Fatalf("mongo retrieve string-id branch failed: %v", err)
	}

	obj.Description = "mongo updated"
	if err := manipulator.Update(
		manipulate.NewContext(context.Background()),
		obj,
	); err != nil {
		t.Fatalf("mongo update string-id branch failed: %v", err)
	}

	if err := manipulator.Delete(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals("mongo-string").Done()),
		),
		obj,
	); err != nil {
		t.Fatalf("mongo delete string-id branch failed: %v", err)
	}

	if _, err := manipulator.Count(
		manipulate.NewContext(context.Background(), manipulate.ContextOptionAfter("not-an-objectid", 3)),
		mongoStringIdentity,
	); err == nil {
		t.Fatalf("expected mongo count invalid-after error")
	}

	cctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	if _, err := manipulator.Count(manipulate.NewContext(cctx), mongoStringIdentity); err == nil {
		t.Fatalf("expected mongo count canceled-context error")
	}

	upsertConvertErr := newMongoStringIDObject("mongo-upsert-convert-error")
	err = manipulator.Create(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(upsertConvertErr.Name).Done()),
			ContextOptionUpsert(mongobson.M{"bad": func() {}}),
		),
		upsertConvertErr,
	)
	if err == nil {
		t.Fatalf("expected mongo upsert encoding error")
	}
}

func TestMongoConstructorAndSpecifierBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)

	if _, err := New(uri, db, OptionCredentials("bad", "bad", "admin"), OptionConnectionTimeout(300*time.Millisecond)); err == nil {
		t.Fatalf("expected mongo constructor ping/auth failure")
	}

	if _, err := New(
		uri,
		db,
		OptionTLS(&tls.Config{InsecureSkipVerify: true}),
		OptionConnectionTimeout(300*time.Millisecond),
		OptionDefaultReadConsistencyMode(manipulate.ReadConsistencyNearest),
		OptionDefaultWriteConsistencyMode(manipulate.WriteConsistencyStrongest),
	); err == nil {
		t.Fatalf("expected mongo TLS branch connection failure for non-TLS memongo endpoint")
	}

	manipulator, err := New(uri, db, OptionForceReadFilter(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator with specifiers: %v", err)
	}
	internalMongo, ok := manipulator.(*mongoDriverManipulator)
	if !ok {
		t.Fatalf("unexpected manipulator type: %T", manipulator)
	}
	internalMongo.attributeSpecifiers = map[elemental.Identity]elemental.AttributeSpecifiable{
		mongoIntegrationIdentity: testmodel.NewTask(),
	}

	task := newMongoIntegrationObject("mongo-spec")
	if err := manipulator.Create(manipulate.NewContext(context.Background()), task); err != nil {
		t.Fatalf("mongo seed create failed: %v", err)
	}

	dest := &mongoIntegrationObject{}
	dest.SetIdentifier(task.Identifier())
	if err := manipulator.Retrieve(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(task.Name).Done()),
			manipulate.ContextOptionFields([]string{"name"}),
		),
		dest,
	); err != nil {
		t.Fatalf("mongo retrieve with specifiers failed: %v", err)
	}

	var list mongoIntegrationObjects
	if err := manipulator.RetrieveMany(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionOrder("name"),
			manipulate.ContextOptionAfter("", 10),
		),
		&list,
	); err != nil {
		t.Fatalf("mongo retrieve many with specifiers failed: %v", err)
	}

	if err := manipulator.DeleteMany(nil, mongoIntegrationIdentity); err != nil {
		t.Fatalf("mongo delete many nil context failed: %v", err)
	}

	if _, err := manipulator.Count(nil, mongoIntegrationIdentity); err != nil {
		t.Fatalf("mongo count nil context failed: %v", err)
	}
}

func TestMongoExplainErrorBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)

	mongoExplain := map[elemental.Identity]map[elemental.Operation]struct{}{
		mongoIntegrationIdentity: {elemental.OperationRetrieve: {}},
	}
	mongoManipulator, err := New(
		uri,
		db,
		OptionExplain(mongoExplain),
		OptionSharder(badFilterMongoSharder{}),
		OptionForceReadFilter(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unable to create mongo explain manipulator: %v", err)
	}
	obj := newMongoIntegrationObject("mongo-explain-error")
	obj.SetIdentifier(mongobson.NewObjectID().Hex())
	if err := mongoManipulator.Retrieve(manipulate.NewContext(context.Background()), obj); err == nil {
		t.Fatalf("expected mongo retrieve explain/query error")
	}
}
