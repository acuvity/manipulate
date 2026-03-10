package manipmongo

import (
	"context"
	"errors"
	"testing"

	"go.acuvity.ai/elemental"
	testmodel "go.acuvity.ai/elemental/test/model"
	"go.acuvity.ai/manipulate"
	bson "go.mongodb.org/mongo-driver/v2/bson"
)

type workerATestSharder struct {
	err        error
	filterOne  bson.D
	filterMany bson.D
	shardCalls int
	writeCalls int
}

func (s *workerATestSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	s.shardCalls++
	return s.err
}

func (s *workerATestSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	s.writeCalls++
	return s.err
}

func (s *workerATestSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (bson.D, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.filterOne, nil
}

func (s *workerATestSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (bson.D, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.filterMany, nil
}

var _ Sharder = (*workerATestSharder)(nil)

func TestWorkerASharderAndFilterHelpers(t *testing.T) {
	sharder := &workerATestSharder{
		filterOne:  bson.D{{Key: "tenant", Value: "acuvity"}},
		filterMany: bson.D{{Key: "tenant", Value: "acuvity"}},
	}
	m := &mongoDriverManipulator{sharder: sharder}
	mctx := manipulate.NewContext(context.Background())
	obj := testmodel.NewTask()
	identity := elemental.MakeIdentity("task", "tasks")

	one, err := makeShardingOneFilter(m, mctx, obj)
	if err != nil {
		t.Fatalf("unexpected sharding one filter error: %v", err)
	}
	if len(one) != 1 || one[0].Key != "tenant" {
		t.Fatalf("unexpected sharding one filter: %#v", one)
	}

	many, err := makeShardingManyFilter(m, mctx, identity)
	if err != nil {
		t.Fatalf("unexpected sharding many filter error: %v", err)
	}
	if len(many) != 1 || many[0].Key != "tenant" {
		t.Fatalf("unexpected sharding many filter: %#v", many)
	}
}

func TestWorkerASharderFilterHelpersWrapErrors(t *testing.T) {
	expectedErr := errors.New("boom")
	m := &mongoDriverManipulator{sharder: &workerATestSharder{err: expectedErr}}
	mctx := manipulate.NewContext(context.Background())
	obj := testmodel.NewTask()
	identity := elemental.MakeIdentity("task", "tasks")

	_, err := makeShardingOneFilter(m, mctx, obj)
	if err == nil {
		t.Fatalf("expected sharding one error")
	}
	var cannotBuild manipulate.ErrCannotBuildQuery
	if !errors.As(err, &cannotBuild) {
		t.Fatalf("expected manipulate.ErrCannotBuildQuery, got %T (%v)", err, err)
	}

	_, err = makeShardingManyFilter(m, mctx, identity)
	if err == nil {
		t.Fatalf("expected sharding many error")
	}
	if !errors.As(err, &cannotBuild) {
		t.Fatalf("expected manipulate.ErrCannotBuildQuery, got %T (%v)", err, err)
	}
}
