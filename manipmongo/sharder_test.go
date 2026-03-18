package manipmongo

import (
	"context"
	"errors"
	"testing"

	"go.acuvity.ai/elemental"
	"go.acuvity.ai/manipulate"
	mongobson "go.mongodb.org/mongo-driver/v2/bson"
)

type testSharder struct {
	filterOne  mongobson.D
	filterMany mongobson.D
	err        error
}

func (*testSharder) Shard(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) error {
	return nil
}
func (*testSharder) OnShardedWrite(manipulate.TransactionalManipulator, manipulate.Context, elemental.Operation, elemental.Identifiable) error {
	return nil
}
func (s *testSharder) FilterOne(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identifiable) (mongobson.D, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.filterOne, nil
}
func (s *testSharder) FilterMany(manipulate.TransactionalManipulator, manipulate.Context, elemental.Identity) (mongobson.D, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.filterMany, nil
}

func TestSharderHelpers(t *testing.T) {
	identity := elemental.MakeIdentity("resource", "resources")
	obj := newMongoIntegrationObject("demo")
	mctx := manipulate.NewContext(context.Background())
	m := &mongoManipulator{sharder: &testSharder{
		filterOne:  mongobson.D{{Key: "tenant", Value: "acuvity"}},
		filterMany: mongobson.D{{Key: "tenant", Value: "acuvity"}},
	}}

	one, err := makeShardingOneFilter(m, mctx, obj)
	if err != nil || len(one) == 0 {
		t.Fatalf("unexpected FilterOne result: %v %v", one, err)
	}
	many, err := makeShardingManyFilter(m, mctx, identity)
	if err != nil || len(many) == 0 {
		t.Fatalf("unexpected FilterMany result: %v %v", many, err)
	}
}

func TestSharderHelpersWrapErrors(t *testing.T) {
	mctx := manipulate.NewContext(context.Background())
	m := &mongoManipulator{sharder: &testSharder{err: errors.New("boom")}}

	if _, err := makeShardingOneFilter(m, mctx, newMongoIntegrationObject("demo")); err == nil {
		t.Fatalf("expected FilterOne error")
	}
	if _, err := makeShardingManyFilter(m, mctx, elemental.MakeIdentity("resource", "resources")); err == nil {
		t.Fatalf("expected FilterMany error")
	}
}
