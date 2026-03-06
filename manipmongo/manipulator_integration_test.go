package manipmongo

import (
	"context"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type pinger interface {
	Ping(timeout time.Duration) error
}

var mongoIntegrationIdentity = elemental.MakeIdentity("mongoitem", "mongoitems")

type mongoIntegrationObject struct {
	ID          mongobson.ObjectID `bson:"_id,omitempty"`
	Name        string             `bson:"name"`
	Description string             `bson:"description"`
	Status      string             `bson:"status"`
	ParentID    string             `bson:"parentid,omitempty"`
	ParentType  string             `bson:"parenttype,omitempty"`
}

func (o *mongoIntegrationObject) Identifier() string {
	if o.ID.IsZero() {
		return ""
	}
	return o.ID.Hex()
}

func (o *mongoIntegrationObject) SetIdentifier(id string) {
	if id == "" {
		o.ID = mongobson.NilObjectID
		return
	}
	oid, err := mongobson.ObjectIDFromHex(id)
	if err != nil {
		o.ID = mongobson.NilObjectID
		return
	}
	o.ID = oid
}
func (o *mongoIntegrationObject) Identity() elemental.Identity      { return mongoIntegrationIdentity }
func (o *mongoIntegrationObject) String() string                    { return o.Name }
func (o *mongoIntegrationObject) Version() int                      { return 0 }
func (o *mongoIntegrationObject) ValueForAttribute(name string) any { return nil }

type mongoIntegrationObjects []*mongoIntegrationObject

func (l mongoIntegrationObjects) Identity() elemental.Identity { return mongoIntegrationIdentity }
func (l mongoIntegrationObjects) List() elemental.IdentifiablesList {
	out := make(elemental.IdentifiablesList, 0, len(l))
	for _, item := range l {
		out = append(out, item)
	}
	return out
}
func (l mongoIntegrationObjects) Copy() elemental.Identifiables {
	cp := make(mongoIntegrationObjects, len(l))
	copy(cp, l)
	return cp
}
func (l mongoIntegrationObjects) Append(objs ...elemental.Identifiable) elemental.Identifiables {
	for _, obj := range objs {
		if item, ok := obj.(*mongoIntegrationObject); ok {
			l = append(l, item)
		}
	}
	return l
}
func (l mongoIntegrationObjects) Version() int { return 0 }

func newMongoIntegrationObject(name string) *mongoIntegrationObject {
	return &mongoIntegrationObject{
		Name:        name,
		Description: "description-" + name,
		Status:      "TODO",
	}
}

func TestOfficialManipulatorCRUDAndHelpersWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)

	manipulator, err := New(
		uri,
		db,
		optionForceReadFilterCanonical(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unable to create official manipulator: %v", err)
	}

	task := newMongoIntegrationObject("mongo-main")
	if err := manipulator.Create(nil, task); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if task.Identifier() == "" {
		t.Fatalf("expected object id to be set")
	}

	retrieved := &mongoIntegrationObject{}
	retrieved.SetIdentifier(task.Identifier())
	if err := manipulator.Retrieve(nil, retrieved); err != nil {
		t.Fatalf("retrieve failed: %v", err)
	}

	task.Description = "updated"
	if err := manipulator.Update(nil, task); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	pageCtx := manipulate.NewContext(
		context.Background(),
		manipulate.ContextOptionOrder("name"),
		manipulate.ContextOptionPage(1, 5),
	)
	var list mongoIntegrationObjects
	if err := manipulator.RetrieveMany(pageCtx, &list); err != nil {
		t.Fatalf("retrievemany failed: %v", err)
	}
	if len(list) == 0 {
		t.Fatalf("expected at least one object in retrievemany")
	}

	count, err := manipulator.Count(manipulate.NewContext(context.Background()), mongoIntegrationIdentity)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count < 1 {
		t.Fatalf("expected count >= 1, got %d", count)
	}

	upsertMongo := newMongoIntegrationObject("mongo-upsert-mongo")
	upsertMongoCtx := manipulate.NewContext(
		context.Background(),
		manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(upsertMongo.Name).Done()),
		contextOptionUpsertCanonical(mongobson.M{}),
	)
	if err := manipulator.Create(upsertMongoCtx, upsertMongo); err != nil {
		t.Fatalf("mongo upsert with mongo bson failed: %v", err)
	}

	if err := manipulator.Delete(nil, task); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	if err := manipulator.DeleteMany(
		manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("status").Equals("TODO").Done()),
		),
		mongoIntegrationIdentity,
	); err != nil {
		t.Fatalf("deletemany failed: %v", err)
	}

	if err := manipulator.(pinger).Ping(5 * time.Second); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	if err := manipulator.Commit(""); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if ok := manipulator.Abort(""); !ok {
		t.Fatalf("abort should return true")
	}

	if !IsMongoManipulator(manipulator) {
		t.Fatalf("official manipulator should satisfy IsMongoManipulator")
	}

	helperIdentity := elemental.MakeIdentity("mongohelpers", "mongohelpers")
	if err := CreateCollection(manipulator, helperIdentity, mongooptions.CreateCollection()); err != nil {
		t.Fatalf("create collection mongo failed: %v", err)
	}

	if err := CreateIndex(
		manipulator,
		helperIdentity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "name", Value: 1}},
			Options: mongooptions.Index().SetName("idx_name"),
		},
	); err != nil {
		t.Fatalf("create indexes mongo failed: %v", err)
	}

	if err := EnsureIndex(
		manipulator,
		helperIdentity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "name", Value: 1}},
			Options: mongooptions.Index().SetName("idx_name").SetUnique(true),
		},
	); err != nil {
		t.Fatalf("ensure indexes mongo conflict reconciliation failed: %v", err)
	}

	if err := EnsureIndex(
		manipulator,
		helperIdentity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "created", Value: 1}},
			Options: mongooptions.Index().SetName("idx_ttl").SetExpireAfterSeconds(30),
		},
	); err != nil {
		t.Fatalf("ensure ttl index mongo failed: %v", err)
	}
	if err := EnsureIndex(
		manipulator,
		helperIdentity,
		mongo.IndexModel{
			Keys:    mongobson.D{{Key: "created", Value: 1}},
			Options: mongooptions.Index().SetName("idx_ttl").SetExpireAfterSeconds(60),
		},
	); err != nil {
		t.Fatalf("ensure ttl index mongo update failed: %v", err)
	}

	dbObj, closeDB, err := getDatabaseCanonical(manipulator)
	if err != nil {
		t.Fatalf("get mongo database failed: %v", err)
	}
	if dbObj == nil {
		t.Fatalf("expected non-nil mongo database")
	}
	defer closeDB()

	if err := setConsistencyModeCanonical(manipulator, manipulate.ReadConsistencyNearest, manipulate.WriteConsistencyStrong); err != nil {
		t.Fatalf("set mongo consistency mode failed: %v", err)
	}
}
