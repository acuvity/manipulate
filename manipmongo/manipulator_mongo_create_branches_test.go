package manipmongo

import (
	"context"
	"testing"

	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

type mongoBadInsertObject struct {
	ID          mongobson.ObjectID `bson:"_id,omitempty"`
	Name        string             `bson:"name"`
	Bad         chan int           `bson:"bad"`
	Description string             `bson:"description"`
}

func (o *mongoBadInsertObject) Identifier() string {
	if o.ID.IsZero() {
		return ""
	}
	return o.ID.Hex()
}
func (o *mongoBadInsertObject) SetIdentifier(id string) {
	oid, err := mongobson.ObjectIDFromHex(id)
	if err != nil {
		o.ID = mongobson.NilObjectID
		return
	}
	o.ID = oid
}
func (o *mongoBadInsertObject) Identity() elemental.Identity { return mongoIntegrationIdentity }
func (o *mongoBadInsertObject) String() string               { return o.Name }
func (o *mongoBadInsertObject) Version() int                 { return 0 }
func (o *mongoBadInsertObject) ValueForAttribute(string) any { return nil }

func TestMongoCreateAdditionalUncoveredBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)

	t.Run("upsert_merge_and_specifier_paths", func(t *testing.T) {
		m, err := New(
			uri,
			db,
			OptionTranslateKeysFromModelManager(testmodel.Manager()),
			OptionSharder(&integrationMongoSharder{tenant: "tenant-create-branches"}),
			OptionForceReadFilter(mongobson.D{{Key: "status", Value: testmodel.TaskStatusTODO}}),
		)
		if err != nil {
			t.Fatalf("unable to create mongo manipulator: %v", err)
		}

		task := newTestTask("mongo-create-upsert-merge")
		ctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(task.Name).Done()),
			manipulate.ContextOptionNamespace("tenant-create-branches"),
			ContextOptionUpsert(mongobson.M{
				"$setOnInsert": mongobson.M{"createdflag": true},
				"$inc":         mongobson.M{"counter": 1},
			}),
		)
		if err := m.Create(ctx, task); err != nil {
			t.Fatalf("upsert merge create failed: %v", err)
		}
		if err := m.Create(ctx, task); err != nil {
			t.Fatalf("upsert merge create second call failed: %v", err)
		}
	})

	t.Run("upsert_string_identifier_branch", func(t *testing.T) {
		m, err := New(uri, db+"_strid")
		if err != nil {
			t.Fatalf("unable to create mongo manipulator: %v", err)
		}

		obj := newMongoStringIDObject("mongo-string-upsert")
		expectedID := "string-upsert-id"
		ctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(obj.Name).Done()),
			contextOptionRawUpsert(mongobson.M{
				"$setOnInsert": mongobson.M{"_id": expectedID},
			}),
		)
		if err := m.Create(ctx, obj); err != nil {
			t.Fatalf("raw upsert string-id create failed: %v", err)
		}
		if obj.Identifier() != expectedID {
			t.Fatalf("expected string upsert identifier %q, got %q", expectedID, obj.Identifier())
		}
	})

	t.Run("upsert_runquery_error_path", func(t *testing.T) {
		m, err := New(
			uri,
			db+"_upsert_err",
			OptionSharder(badFilterMongoSharder{}),
			OptionForceReadFilter(mongobson.D{}),
		)
		if err != nil {
			t.Fatalf("unable to create mongo manipulator: %v", err)
		}

		obj := newMongoIntegrationObject("mongo-upsert-runquery-error")
		ctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(obj.Name).Done()),
			ContextOptionUpsert(mongobson.M{}),
		)
		if err := m.Create(ctx, obj); err == nil {
			t.Fatalf("expected upsert runquery error")
		}
	})

	t.Run("insert_runquery_error_path", func(t *testing.T) {
		m, err := New(uri, db+"_insert_err", OptionForceReadFilter(mongobson.D{}))
		if err != nil {
			t.Fatalf("unable to create mongo manipulator: %v", err)
		}

		obj := &mongoBadInsertObject{
			Name:        "mongo-insert-error",
			Description: "mongo-insert-error-desc",
			Bad:         make(chan int),
		}
		if err := m.Create(manipulate.NewContext(context.Background()), obj); err == nil {
			t.Fatalf("expected insert runquery error")
		}
	})
}
