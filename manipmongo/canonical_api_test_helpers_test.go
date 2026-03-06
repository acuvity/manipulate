package manipmongo

import (
	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func optionForceReadFilterCanonical(filter mongobson.D) Option {
	return OptionForceReadFilter(filter)
}

func contextOptionUpsertCanonical(operations mongobson.M) manipulate.ContextOption {
	return ContextOptionUpsert(operations)
}

func createIndexCanonical(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error {
	return CreateIndex(manipulator, identity, indexes...)
}

func ensureIndexCanonical(manipulator manipulate.Manipulator, identity elemental.Identity, indexes ...mongo.IndexModel) error {
	return EnsureIndex(manipulator, identity, indexes...)
}

func createCollectionCanonical(
	manipulator manipulate.Manipulator,
	identity elemental.Identity,
	opts ...mongooptions.Lister[mongooptions.CreateCollectionOptions],
) error {
	return CreateCollection(manipulator, identity, opts...)
}

func getDatabaseCanonical(manipulator manipulate.Manipulator) (*mongo.Database, func(), error) {
	db, err := GetDatabase(manipulator)
	return db, func() {}, err
}

func setConsistencyModeCanonical(
	manipulator manipulate.Manipulator,
	readConsistency manipulate.ReadConsistency,
	writeConsistency manipulate.WriteConsistency,
) error {
	return SetConsistencyMode(manipulator, readConsistency, writeConsistency)
}

type integrationMongoSharder struct {
	tenant string
}

func (s *integrationMongoSharder) Shard(_ manipulate.TransactionalManipulator, _ manipulate.Context, obj elemental.Identifiable) error {
	if task, ok := obj.(*testmodel.Task); ok {
		task.ParentID = s.tenant
		task.ParentType = "tenant"
	}
	return nil
}

func (s *integrationMongoSharder) OnShardedWrite(_ manipulate.TransactionalManipulator, _ manipulate.Context, _ elemental.Operation, _ elemental.Identifiable) error {
	return nil
}

func (s *integrationMongoSharder) FilterOne(_ manipulate.TransactionalManipulator, _ manipulate.Context, _ elemental.Identifiable) (mongobson.D, error) {
	return mongobson.D{{Key: "parentid", Value: s.tenant}}, nil
}

func (s *integrationMongoSharder) FilterMany(_ manipulate.TransactionalManipulator, _ manipulate.Context, _ elemental.Identity) (mongobson.D, error) {
	return mongobson.D{{Key: "parentid", Value: s.tenant}}, nil
}

func newTestTask(name string) *testmodel.Task {
	t := testmodel.NewTask()
	t.Name = name
	t.Description = "description-" + name
	t.Status = testmodel.TaskStatusTODO
	return t
}
