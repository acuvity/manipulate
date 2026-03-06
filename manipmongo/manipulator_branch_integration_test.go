package manipmongo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

type writeErrMongoSharder struct{}

func (writeErrMongoSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}
func (writeErrMongoSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return fmt.Errorf("write hook error")
}
func (writeErrMongoSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (mongobson.D, error) {
	return mongobson.D{}, nil
}
func (writeErrMongoSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (mongobson.D, error) {
	return mongobson.D{}, nil
}

type badFilterMongoSharder struct{}

func (badFilterMongoSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}
func (badFilterMongoSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return nil
}
func (badFilterMongoSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (mongobson.D, error) {
	return mongobson.D{{Key: "$set", Value: mongobson.M{"x": 1}}}, nil
}
func (badFilterMongoSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (mongobson.D, error) {
	return mongobson.D{{Key: "$set", Value: mongobson.M{"x": 1}}}, nil
}

var mongoCryptoIdentity = elemental.MakeIdentity("mongocryptotask", "mongocryptotasks")

type mongoCryptoObject struct {
	ID          mongobson.ObjectID `bson:"_id,omitempty"`
	Name        string             `bson:"name"`
	Description string             `bson:"description"`
	ParentID    string             `bson:"parentid,omitempty"`
	ParentType  string             `bson:"parenttype,omitempty"`
	FailEncrypt bool               `bson:"-"`
	FailDecrypt bool               `bson:"-"`
}

func (o *mongoCryptoObject) Identifier() string {
	if o.ID.IsZero() {
		return ""
	}
	return o.ID.Hex()
}

func (o *mongoCryptoObject) SetIdentifier(id string) {
	oid, err := mongobson.ObjectIDFromHex(id)
	if err != nil {
		o.ID = mongobson.NilObjectID
		return
	}
	o.ID = oid
}

func (o *mongoCryptoObject) Identity() elemental.Identity { return mongoCryptoIdentity }
func (o *mongoCryptoObject) String() string               { return o.Name }
func (o *mongoCryptoObject) Version() int                 { return 0 }
func (o *mongoCryptoObject) ValueForAttribute(string) any { return nil }

func (o *mongoCryptoObject) EncryptAttributes(elemental.AttributeEncrypter) error {
	if o.FailEncrypt {
		return fmt.Errorf("encrypt failed")
	}
	return nil
}

func (o *mongoCryptoObject) DecryptAttributes(elemental.AttributeEncrypter) error {
	if o.FailDecrypt {
		return fmt.Errorf("decrypt failed")
	}
	return nil
}

type mongoCryptoObjects []*mongoCryptoObject

func (l mongoCryptoObjects) Identity() elemental.Identity { return mongoCryptoIdentity }

func (l mongoCryptoObjects) List() elemental.IdentifiablesList {
	out := make(elemental.IdentifiablesList, 0, len(l))
	for _, item := range l {
		out = append(out, item)
	}
	return out
}

func (l mongoCryptoObjects) Copy() elemental.Identifiables {
	cp := make(mongoCryptoObjects, len(l))
	copy(cp, l)
	return cp
}

func (l mongoCryptoObjects) Append(objs ...elemental.Identifiable) elemental.Identifiables {
	for _, obj := range objs {
		if item, ok := obj.(*mongoCryptoObject); ok {
			l = append(l, item)
		}
	}
	return l
}

func (l mongoCryptoObjects) Version() int { return 0 }

func contextOptionRawUpsert(v any) manipulate.ContextOption {
	return func(c manipulate.Context) {
		c.(opaquer).Opaque()[opaqueKeyUpsert] = v
	}
}

func TestMongoManipulatorBranchCoverageWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)

	manipulator, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	t.Run("create_finalizer_error", func(t *testing.T) {
		task := newMongoIntegrationObject("mongo-finalizer")
		err := manipulator.Create(
			manipulate.NewContext(
				context.Background(),
				manipulate.ContextOptionFinalizer(func(elemental.Identifiable) error {
					return errors.New("finalizer failed")
				}),
			),
			task,
		)
		if err == nil || !strings.Contains(err.Error(), "finalizer failed") {
			t.Fatalf("expected finalizer error, got %v", err)
		}
	})

	t.Run("create_upsert_invalid_type", func(t *testing.T) {
		task := newMongoIntegrationObject("mongo-raw-upsert")
		ctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(task.Name).Done()),
			contextOptionRawUpsert(42),
		)
		err := manipulator.Create(ctx, task)
		if err == nil || !strings.Contains(err.Error(), "upsert operations must be bson.M") {
			t.Fatalf("expected invalid upsert type error, got %v", err)
		}
	})

	t.Run("create_upsert_invalid_set_on_insert_type", func(t *testing.T) {
		task := newMongoIntegrationObject("mongo-raw-upsert-invalid-soi")
		ctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(task.Name).Done()),
			contextOptionRawUpsert(mongobson.M{"$setOnInsert": "bad"}),
		)
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("expected upsert invalid $setOnInsert type to return an error, got panic: %v", recovered)
			}
		}()

		err := manipulator.Create(ctx, task)
		if err == nil || !strings.Contains(err.Error(), "$setOnInsert") {
			t.Fatalf("expected invalid $setOnInsert type error, got %v", err)
		}
	})

	t.Run("create_upsert_nil_ops", func(t *testing.T) {
		task := newMongoIntegrationObject("mongo-upsert-nil")
		ctx := manipulate.NewContext(
			context.Background(),
			manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(task.Name).Done()),
			ContextOptionUpsert(nil),
		)
		if err := manipulator.Create(ctx, task); err != nil {
			t.Fatalf("expected nil upsert ops to succeed, got %v", err)
		}
		if task.Identifier() == "" {
			t.Fatalf("expected identifier to be set for nil upsert ops create")
		}
	})

	t.Run("retrieve_many_nil_context_and_next_token", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			task := newMongoIntegrationObject(fmt.Sprintf("mongo-list-%d", i))
			if err := manipulator.Create(manipulate.NewContext(context.Background()), task); err != nil {
				t.Fatalf("seed create failed: %v", err)
			}
		}

		var all mongoIntegrationObjects
		if err := manipulator.RetrieveMany(nil, &all); err != nil {
			t.Fatalf("retrieve many with nil context failed: %v", err)
		}

		pageCtx := manipulate.NewContext(context.Background(), manipulate.ContextOptionAfter("", 1))
		var page mongoIntegrationObjects
		if err := manipulator.RetrieveMany(pageCtx, &page); err != nil {
			t.Fatalf("paged retrieve many failed: %v", err)
		}
		if len(page) != 1 {
			t.Fatalf("expected exactly one paged item, got %d", len(page))
		}
		if pageCtx.Next() == "" {
			t.Fatalf("expected next token to be populated")
		}
	})

	t.Run("retrieve_canceled_context", func(t *testing.T) {
		task := newMongoIntegrationObject("mongo-canceled-retrieve")
		if err := manipulator.Create(manipulate.NewContext(context.Background()), task); err != nil {
			t.Fatalf("seed create failed: %v", err)
		}

		cctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		time.Sleep(time.Millisecond)
		dest := &mongoIntegrationObject{}
		dest.SetIdentifier(task.Identifier())
		err := manipulator.Retrieve(manipulate.NewContext(cctx), dest)
		if err == nil {
			t.Fatalf("expected canceled-context retrieve error")
		}
	})

	t.Run("update_delete_missing_object", func(t *testing.T) {
		missing := newMongoIntegrationObject("mongo-missing")
		missing.SetIdentifier(mongobson.NewObjectID().Hex())

		err := manipulator.Update(
			manipulate.NewContext(context.Background(), manipulate.ContextOptionNamespace("ns-mongo")),
			missing,
		)
		if err == nil || !manipulate.IsObjectNotFoundError(err) {
			t.Fatalf("expected update object-not-found error, got %v", err)
		}

		err = manipulator.Delete(
			manipulate.NewContext(
				context.Background(),
				manipulate.ContextOptionNamespace("ns-mongo"),
				manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(missing.Name).Done()),
			),
			missing,
		)
		if err == nil || !manipulate.IsObjectNotFoundError(err) {
			t.Fatalf("expected delete object-not-found error, got %v", err)
		}
	})

	t.Run("count_empty_collection", func(t *testing.T) {
		_, dbEmpty := requireMemongo(t)
		emptyManipulator, err := New(uri, dbEmpty, optionForceReadFilterCanonical(mongobson.D{}))
		if err != nil {
			t.Fatalf("unable to create empty mongo manipulator: %v", err)
		}
		count, err := emptyManipulator.Count(manipulate.NewContext(context.Background()), mongoIntegrationIdentity)
		if err != nil {
			t.Fatalf("count failed: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected empty count 0, got %d", count)
		}
	})

	t.Run("on_sharded_write_errors", func(t *testing.T) {
		withWriteErr, err := New(
			uri,
			db,
			OptionSharder(writeErrMongoSharder{}),
			optionForceReadFilterCanonical(mongobson.D{}),
		)
		if err != nil {
			t.Fatalf("unable to create mongo manipulator with write error sharder: %v", err)
		}

		task := newMongoIntegrationObject("mongo-write-hook")
		err = withWriteErr.Create(manipulate.NewContext(context.Background()), task)
		if err == nil || !strings.Contains(err.Error(), "OnShardedWrite on create") {
			t.Fatalf("expected create on-sharded-write error, got %v", err)
		}

		delTask := newMongoIntegrationObject("mongo-delete-write-hook")
		if err := manipulator.Create(manipulate.NewContext(context.Background()), delTask); err != nil {
			t.Fatalf("seed create failed: %v", err)
		}
		err = withWriteErr.Delete(manipulate.NewContext(context.Background()), delTask)
		if err == nil || !strings.Contains(err.Error(), "OnShardedWrite for delete") {
			t.Fatalf("expected delete on-sharded-write error, got %v", err)
		}
	})
}

func TestMongoManipulatorCanceledOperationContextsWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)

	manipulator, err := New(uri, db, optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	seed := newMongoIntegrationObject("mongo-canceled-op-seed")
	if err := manipulator.Create(manipulate.NewContext(context.Background()), seed); err != nil {
		t.Fatalf("seed create failed: %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	canceledCtx := manipulate.NewContext(canceled)

	retrieveTarget := &mongoIntegrationObject{}
	retrieveTarget.SetIdentifier(seed.Identifier())
	if err := manipulator.Retrieve(canceledCtx, retrieveTarget); err == nil {
		t.Fatalf("expected retrieve to fail on canceled context")
	}

	var list mongoIntegrationObjects
	if err := manipulator.RetrieveMany(canceledCtx, &list); err == nil {
		t.Fatalf("expected retrieve many to fail on canceled context")
	}

	seed.Description = "updated"
	if err := manipulator.Update(canceledCtx, seed); err == nil {
		t.Fatalf("expected update to fail on canceled context")
	}

	if err := manipulator.Delete(canceledCtx, seed); err == nil {
		t.Fatalf("expected delete to fail on canceled context")
	}

	if err := manipulator.DeleteMany(canceledCtx, mongoIntegrationIdentity); err == nil {
		t.Fatalf("expected delete many to fail on canceled context")
	}

	if _, err := manipulator.Count(canceledCtx, mongoIntegrationIdentity); err == nil {
		t.Fatalf("expected count to fail on canceled context")
	}

	createTarget := newMongoIntegrationObject("mongo-canceled-create")
	if err := manipulator.Create(canceledCtx, createTarget); err == nil {
		t.Fatalf("expected create to fail on canceled context")
	}

	createUpsertTarget := newMongoIntegrationObject("mongo-canceled-create-upsert")
	createUpsertCtx := manipulate.NewContext(
		canceled,
		manipulate.ContextOptionFilter(elemental.NewFilterComposer().WithKey("name").Equals(createUpsertTarget.Name).Done()),
		contextOptionUpsertCanonical(mongobson.M{}),
	)
	if err := manipulator.Create(createUpsertCtx, createUpsertTarget); err == nil {
		t.Fatalf("expected create upsert to fail on canceled context")
	}
}

func TestMongoManipulatorEncryptionAndErrorBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)
	enc, err := elemental.NewAESAttributeEncrypter("0123456789ABCDEF")
	if err != nil {
		t.Fatalf("unable to build encrypter: %v", err)
	}

	manipulator, err := New(
		uri,
		db,
		OptionAttributeEncrypter(enc),
		optionForceReadFilterCanonical(mongobson.D{}),
	)
	if err != nil {
		t.Fatalf("unable to create mongo manipulator: %v", err)
	}

	t.Run("create_encrypt_error", func(t *testing.T) {
		obj := &mongoCryptoObject{Name: "mongo-enc-fail", FailEncrypt: true}
		err := manipulator.Create(manipulate.NewContext(context.Background()), obj)
		if err == nil || !strings.Contains(err.Error(), "create: unable to encrypt attributes") {
			t.Fatalf("expected encrypt error, got %v", err)
		}
	})

	t.Run("create_decrypt_error", func(t *testing.T) {
		obj := &mongoCryptoObject{Name: "mongo-dec-fail", FailDecrypt: true}
		err := manipulator.Create(manipulate.NewContext(context.Background()), obj)
		if err == nil || !strings.Contains(err.Error(), "create: unable to decrypt attributes") {
			t.Fatalf("expected decrypt error, got %v", err)
		}
	})

	t.Run("retrieve_decrypt_error", func(t *testing.T) {
		seed := &mongoCryptoObject{Name: "mongo-retrieve-dec"}
		if err := manipulator.Create(manipulate.NewContext(context.Background()), seed); err != nil {
			t.Fatalf("seed create failed: %v", err)
		}
		dest := &mongoCryptoObject{FailDecrypt: true}
		dest.SetIdentifier(seed.Identifier())
		err := manipulator.Retrieve(manipulate.NewContext(context.Background()), dest)
		if err == nil || !strings.Contains(err.Error(), "retrieve: unable to decrypt attributes") {
			t.Fatalf("expected retrieve decrypt error, got %v", err)
		}
	})

	t.Run("retrieve_many_decrypt_error", func(t *testing.T) {
		seed := &mongoCryptoObject{Name: "mongo-list-dec"}
		if err := manipulator.Create(manipulate.NewContext(context.Background()), seed); err != nil {
			t.Fatalf("seed create failed: %v", err)
		}
		var list mongoCryptoObjects
		for i := 0; i < 1; i++ {
			list = append(list, &mongoCryptoObject{FailDecrypt: true})
		}
		err := manipulator.RetrieveMany(manipulate.NewContext(context.Background()), &list)
		if err == nil || !strings.Contains(err.Error(), "retrievemany: unable to decrypt attributes") {
			t.Fatalf("expected retrieve many decrypt error, got %v", err)
		}
	})

	t.Run("update_encrypt_and_decrypt_errors", func(t *testing.T) {
		seed := &mongoCryptoObject{Name: "mongo-update-dec"}
		if err := manipulator.Create(manipulate.NewContext(context.Background()), seed); err != nil {
			t.Fatalf("seed create failed: %v", err)
		}

		encFail := &mongoCryptoObject{Name: "mongo-update-enc-fail", FailEncrypt: true}
		encFail.SetIdentifier(seed.Identifier())
		err := manipulator.Update(manipulate.NewContext(context.Background()), encFail)
		if err == nil || !strings.Contains(err.Error(), "update: unable to encrypt attributes") {
			t.Fatalf("expected update encrypt error, got %v", err)
		}

		decFail := &mongoCryptoObject{Name: "mongo-update-dec-fail", FailDecrypt: true}
		decFail.SetIdentifier(seed.Identifier())
		err = manipulator.Update(manipulate.NewContext(context.Background()), decFail)
		if err == nil || !strings.Contains(err.Error(), "update: unable to decrypt attributes") {
			t.Fatalf("expected update decrypt error, got %v", err)
		}
	})

	t.Run("count_and_delete_many_error_paths", func(t *testing.T) {
		withBadFilter, err := New(
			uri,
			db,
			OptionSharder(badFilterMongoSharder{}),
			optionForceReadFilterCanonical(mongobson.D{}),
		)
		if err != nil {
			t.Fatalf("unable to create bad-filter mongo manipulator: %v", err)
		}

		if _, err := withBadFilter.Count(manipulate.NewContext(context.Background()), mongoCryptoIdentity); err == nil {
			t.Fatalf("expected count bad-filter error")
		}
		var list mongoCryptoObjects
		if err := withBadFilter.RetrieveMany(manipulate.NewContext(context.Background()), &list); err == nil {
			t.Fatalf("expected retrieve many bad-filter error")
		}
		if err := withBadFilter.DeleteMany(manipulate.NewContext(context.Background()), mongoCryptoIdentity); err == nil {
			t.Fatalf("expected delete many bad-filter error")
		}
	})
}

func TestMongoManipulatorExplainBranchesWithMemongo(t *testing.T) {
	uri, db := requireMemongo(t)
	explainOps := map[elemental.Identity]map[elemental.Operation]struct{}{
		mongoIntegrationIdentity: {
			elemental.OperationRetrieve:     {},
			elemental.OperationRetrieveMany: {},
			elemental.OperationInfo:         {},
		},
	}
	manipulator, err := New(uri, db, OptionExplain(explainOps), optionForceReadFilterCanonical(mongobson.D{}))
	if err != nil {
		t.Fatalf("unable to create mongo manipulator with explain: %v", err)
	}

	task := newMongoIntegrationObject("mongo-explain")
	if err := manipulator.Create(manipulate.NewContext(context.Background()), task); err != nil {
		t.Fatalf("seed create failed: %v", err)
	}

	dest := &mongoIntegrationObject{}
	dest.SetIdentifier(task.Identifier())
	if err := manipulator.Retrieve(manipulate.NewContext(context.Background()), dest); err != nil {
		t.Fatalf("retrieve with explain failed: %v", err)
	}
	var list mongoIntegrationObjects
	if err := manipulator.RetrieveMany(manipulate.NewContext(context.Background()), &list); err != nil {
		t.Fatalf("retrieve many with explain failed: %v", err)
	}
	if _, err := manipulator.Count(manipulate.NewContext(context.Background()), mongoIntegrationIdentity); err != nil {
		t.Fatalf("count with explain failed: %v", err)
	}
}
