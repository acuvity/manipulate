package manipmongo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.acuvity.ai/elemental"
	bson "go.mongodb.org/mongo-driver/v2/bson"
)

type workerAExplainableStub struct {
	lastResult any
	err        error
}

func (s *workerAExplainableStub) Explain(result interface{}) error {
	s.lastResult = result
	return s.err
}

type workerAFakeQuery struct {
	doc bson.M
	err error
}

func (q *workerAFakeQuery) One(out any) error {
	if q.err != nil {
		return q.err
	}
	target, ok := out.(*bson.M)
	if !ok {
		return errors.New("invalid output type")
	}
	*target = q.doc
	return nil
}

type workerAFakeCollection struct {
	query  *workerAFakeQuery
	lastID string
}

func (c *workerAFakeCollection) FindId(id string) *workerAFakeQuery {
	c.lastID = id
	return c.query
}

func TestWorkerASpanErrAndExplainHelpers(t *testing.T) {
	sp := opentracing.NoopTracer{}.StartSpan("worker-a")
	err := spanErr(sp, errors.New("boom"))
	if err == nil || err.Error() != "boom" {
		t.Fatalf("unexpected spanErr result: %v", err)
	}

	identity := elemental.MakeIdentity("thing", "things")
	explainable := &workerAExplainableStub{}
	callback := explainIfNeeded(
		explainable,
		bson.D{{Key: "_id", Value: "1"}},
		identity,
		elemental.OperationRetrieve,
		map[elemental.Identity]map[elemental.Operation]struct{}{
			identity: {elemental.OperationRetrieve: {}},
		},
	)
	if callback == nil {
		t.Fatalf("expected explain callback")
	}
	if err := callback(); err != nil {
		t.Fatalf("unexpected explain callback error: %v", err)
	}
	if explainable.lastResult == nil {
		t.Fatalf("expected explain result to be set")
	}

	if explainIfNeeded(explainable, nil, identity, elemental.OperationCreate, nil) != nil {
		t.Fatalf("expected nil callback when explain map is empty")
	}
}

func TestWorkerAMakePreviousRetrieverReflectionPath(t *testing.T) {
	id := bson.NewObjectID()
	fake := &workerAFakeCollection{
		query: &workerAFakeQuery{doc: bson.M{"name": "acuvity"}},
	}

	retriever := makePreviousRetriever(fake)
	doc, err := retriever(id)
	if err != nil {
		t.Fatalf("unexpected retriever error: %v", err)
	}
	if doc["name"] != "acuvity" {
		t.Fatalf("unexpected document: %#v", doc)
	}
	if fake.lastID != id.Hex() {
		t.Fatalf("unexpected converted id passed to reflection retriever: got %q want %q", fake.lastID, id.Hex())
	}

	nilRetriever := makePreviousRetriever(nil)
	if _, err := nilRetriever(id); err == nil || !strings.Contains(err.Error(), "collection is nil") {
		t.Fatalf("expected collection is nil error, got %v", err)
	}
}

func TestWorkerARedactMongoURIAndOperationContext(t *testing.T) {
	redacted := redactMongoURI("mongodb://user:secret@localhost:27017/db")
	if strings.Contains(redacted, "secret") {
		t.Fatalf("expected password to be redacted, got %q", redacted)
	}
	if !strings.Contains(redacted, "REDACTED") {
		t.Fatalf("expected redacted marker in URI, got %q", redacted)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	opCtx, opCancel, err := mongoOperationContext(ctx)
	if err != nil {
		t.Fatalf("unexpected mongoOperationContext error: %v", err)
	}
	defer opCancel()
	if deadline, ok := opCtx.Deadline(); !ok || deadline.IsZero() {
		t.Fatalf("expected derived operation context with deadline")
	}
}
