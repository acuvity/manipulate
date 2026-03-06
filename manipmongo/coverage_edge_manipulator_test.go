package manipmongo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

type mongoSpecObject struct {
	ID          mongobson.ObjectID `bson:"_id,omitempty"`
	Name        string             `bson:"name"`
	Description string             `bson:"description"`
	ParentID    string             `bson:"parentid,omitempty"`
	ParentType  string             `bson:"parenttype,omitempty"`
}

func (o *mongoSpecObject) Identifier() string {
	if o.ID.IsZero() {
		return ""
	}
	return o.ID.Hex()
}

func (o *mongoSpecObject) SetIdentifier(id string) {
	oid, err := mongobson.ObjectIDFromHex(id)
	if err != nil {
		o.ID = mongobson.NilObjectID
		return
	}
	o.ID = oid
}

func (o *mongoSpecObject) Identity() elemental.Identity { return mongoIntegrationIdentity }
func (o *mongoSpecObject) String() string               { return o.Name }
func (o *mongoSpecObject) Version() int                 { return 0 }

func (o *mongoSpecObject) ValueForAttribute(name string) any {
	switch name {
	case "name":
		return o.Name
	case "description":
		return o.Description
	default:
		return nil
	}
}

func (o *mongoSpecObject) SpecificationForAttribute(name string) elemental.AttributeSpecification {
	switch name {
	case "name":
		return elemental.AttributeSpecification{BSONFieldName: "name"}
	case "description":
		return elemental.AttributeSpecification{BSONFieldName: "description"}
	default:
		return elemental.AttributeSpecification{}
	}
}

func (o *mongoSpecObject) AttributeSpecifications() map[string]elemental.AttributeSpecification {
	return map[string]elemental.AttributeSpecification{
		"name":        {BSONFieldName: "name"},
		"description": {BSONFieldName: "description"},
	}
}

type mongoSpecObjects []*mongoSpecObject

func (l mongoSpecObjects) Identity() elemental.Identity { return mongoIntegrationIdentity }

func (l mongoSpecObjects) List() elemental.IdentifiablesList {
	out := make(elemental.IdentifiablesList, 0, len(l))
	for _, item := range l {
		out = append(out, item)
	}
	return out
}

func (l mongoSpecObjects) Copy() elemental.Identifiables {
	cp := make(mongoSpecObjects, len(l))
	copy(cp, l)
	return cp
}

func (l mongoSpecObjects) Append(objs ...elemental.Identifiable) elemental.Identifiables {
	for _, obj := range objs {
		if item, ok := obj.(*mongoSpecObject); ok {
			l = append(l, item)
		}
	}
	return l
}

func (l mongoSpecObjects) Version() int           { return 0 }
func (l mongoSpecObjects) DefaultOrder() []string { return []string{"name"} }

type upsertFilterErrMongoSharder struct{}

func (upsertFilterErrMongoSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}

func (upsertFilterErrMongoSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return nil
}

func (upsertFilterErrMongoSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (mongobson.D, error) {
	return nil, errors.New("upsert filter error")
}

func (upsertFilterErrMongoSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (mongobson.D, error) {
	return nil, nil
}

func TestCoverageMongoManipulatorAdditionalBranches(t *testing.T) {
	uri, db := requireMemongo(t)

	mapi, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}
	mongoManip := mapi.(*mongoDriverManipulator)
	mongoManip.attributeSpecifiers = map[elemental.Identity]elemental.AttributeSpecifiable{
		mongoIntegrationIdentity: &mongoSpecObject{},
	}

	if err := mongoManip.Create(manipulate.NewContext(context.Background()), &mongoSpecObject{Name: "mongo-spec-one", Description: "d1"}); err != nil {
		t.Fatalf("unable to seed mongo spec object one: %v", err)
	}
	if err := mongoManip.Create(manipulate.NewContext(context.Background()), &mongoSpecObject{Name: "mongo-spec-two", Description: "d2"}); err != nil {
		t.Fatalf("unable to seed mongo spec object two: %v", err)
	}

	var ordered mongoSpecObjects
	if err := mongoManip.RetrieveMany(manipulate.NewContext(context.Background()), &ordered); err != nil {
		t.Fatalf("expected default-order retrieve many success, got %v", err)
	}
	if len(ordered) == 0 {
		t.Fatalf("expected retrieve many results")
	}

	retrieveDest := &mongoSpecObject{}
	retrieveDest.SetIdentifier(ordered[0].Identifier())
	if err := mongoManip.Retrieve(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("bad").Equals(func() {}).Done()),
		),
		retrieveDest,
	); err == nil {
		t.Fatalf("expected retrieve query build/execution error for bad filter value")
	}

	var badPipelineList mongoSpecObjects
	if err := mongoManip.RetrieveMany(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("bad").Equals(func() {}).Done()),
		),
		&badPipelineList,
	); err == nil {
		t.Fatalf("expected retrieve many error for bad filter value")
	}

	var invalidAfter mongoSpecObjects
	if err := mongoManip.RetrieveMany(
		manipulate.NewContext(context.Background(), manipulate.ContextOptionAfter("not-a-valid-objectid", 3)),
		&invalidAfter,
	); err == nil {
		t.Fatalf("expected retrieve many invalid-after error")
	}

	cctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	var canceledList mongoSpecObjects
	if err := mongoManip.RetrieveMany(manipulate.NewContext(cctx), &canceledList); err == nil {
		t.Fatalf("expected retrieve many canceled-context error")
	}

	if err := mongoManip.Retrieve(
		manipulate.NewContext(context.Background(), manipulate.ContextOptionNamespace("mongo-retrieve-ns")),
		retrieveDest,
	); err == nil {
		t.Fatalf("expected retrieve namespace filter mismatch error")
	}

	badUpdater := &mongoBadInsertObject{Name: "mongo-bad-update", Bad: make(chan int)}
	badUpdater.SetIdentifier(ordered[0].Identifier())
	if err := mongoManip.Update(manipulate.NewContext(context.Background()), badUpdater); err == nil {
		t.Fatalf("expected update encoding error")
	}

	withSharderUpsert, err := New(
		uri,
		db+"_upsert_filter_convert",
		OptionSharder(&integrationMongoSharder{tenant: "tenant-upsert"}),
		optionForceReadFilterCanonical(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unable to create upsert conversion manipulator: %v", err)
	}
	upsertFilterObj := newTestTask("mongo-upsert-convert-filter")
	if err := withSharderUpsert.Create(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("bad").Equals(func() {}).Done()),
			contextOptionUpsertCanonical(mongobson.M{}),
		),
		upsertFilterObj,
	); err == nil {
		t.Fatalf("expected create upsert error for bad filter value")
	}

	withUpsertFilterErr, err := New(
		uri,
		db+"_upsert_filter_only_err",
		OptionSharder(upsertFilterErrMongoSharder{}),
		optionForceReadFilterCanonical(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unable to create upsert filter error manipulator: %v", err)
	}
	upsertFilterOnlyObj := newMongoIntegrationObject("mongo-upsert-filter-only-error")
	if err := withUpsertFilterErr.Create(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(upsertFilterOnlyObj.Name).Done()),
			contextOptionUpsertCanonical(mongobson.M{}),
		),
		upsertFilterOnlyObj,
	); err == nil || !strings.Contains(err.Error(), "cannot compute sharding filter") {
		t.Fatalf("expected upsert filter-one error branch, got %v", err)
	}

	withSharderUpdate, err := New(
		uri,
		db+"_update_sharder",
		OptionSharder(&integrationMongoSharder{tenant: "tenant-update"}),
		optionForceReadFilterCanonical(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unable to create update sharder manipulator: %v", err)
	}
	updateSeed := newTestTask("mongo-update-sharder-seed")
	if err := withSharderUpdate.Create(manipulate.NewContext(context.Background()), updateSeed); err != nil {
		t.Fatalf("unable to seed update sharder object: %v", err)
	}
	updateSeed.Description = "updated"
	if err := withSharderUpdate.Update(manipulate.NewContext(context.Background()), updateSeed); err == nil {
		t.Fatalf("expected update with mismatched shard data to fail")
	}

	withBadDelete, err := New(uri, db+"_bad_delete", OptionSharder(badFilterMongoSharder{}), optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create bad-delete manipulator: %v", err)
	}
	badDeleteObj := newMongoIntegrationObject("mongo-bad-delete")
	badDeleteObj.SetIdentifier(mongobson.NewObjectID().Hex())
	if err := withBadDelete.Delete(manipulate.NewContext(context.Background()), badDeleteObj); err == nil {
		t.Fatalf("expected delete command/query error")
	}

	deleteSpecObj := &mongoSpecObject{Name: "mongo-delete-spec", Description: "desc"}
	if err := mongoManip.Create(manipulate.NewContext(context.Background()), deleteSpecObj); err != nil {
		t.Fatalf("unable to seed delete spec object: %v", err)
	}
	if err := mongoManip.Delete(manipulate.NewContext(context.Background()), deleteSpecObj); err != nil {
		t.Fatalf("expected delete success, got %v", err)
	}
	if err := mongoManip.Delete(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("bad").Equals(func() {}).Done()),
		),
		badDeleteObj,
	); err == nil {
		t.Fatalf("expected delete error for bad filter value")
	}

	if err := mongoManip.DeleteMany(
		manipulate.NewContext(context.Background(), manipulate.ContextOptionNamespace("mongo-delete-many-ns")),
		mongoIntegrationIdentity,
	); err != nil {
		t.Fatalf("expected delete many namespace branch success, got %v", err)
	}

	if err := mongoManip.DeleteMany(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("bad").Equals(func() {}).Done()),
		),
		mongoIntegrationIdentity,
	); err == nil {
		t.Fatalf("expected delete many error for bad filter value")
	}

	if _, err := mongoManip.Count(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("bad").Equals(func() {}).Done()),
		),
		mongoIntegrationIdentity,
	); err == nil {
		t.Fatalf("expected count error for bad filter value")
	}

	if err := HandleQueryErrorMongo(errors.New("lost connection to server")); !manipulate.IsCannotCommunicateError(err) {
		t.Fatalf("expected connection classification branch, got %v", err)
	}
}
