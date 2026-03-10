package manipmongo

import (
	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

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
