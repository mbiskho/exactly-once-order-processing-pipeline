package durable

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestRandomIDUsesPrefixAndUniqueEntropy(t *testing.T) {
	first, err := randomID("evt")
	if err != nil {
		t.Fatal(err)
	}
	second, err := randomID("evt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first, "evt-") || first == second {
		t.Fatalf("generated IDs %q and %q; want unique evt-prefixed IDs", first, second)
	}
}

func TestPermanentErrorCanBeClassified(t *testing.T) {
	err := PermanentError{Err: errors.New("malformed event")}
	var permanent PermanentError
	if !errors.As(err, &permanent) {
		t.Fatal("PermanentError could not be classified by errors.As")
	}
	if permanent.Error() != "malformed event" {
		t.Fatalf("error = %q, want wrapped message", permanent.Error())
	}
}

func TestUniqueViolationClassification(t *testing.T) {
	if !isUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("PostgreSQL unique violation was not classified")
	}
	if isUniqueViolation(&pgconn.PgError{Code: "40001"}) {
		t.Fatal("serialization failure was classified as a unique violation")
	}
}

func TestOpenRequiresDurableDependencies(t *testing.T) {
	_, err := Open(context.Background(), Config{})
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("Open() error = %v, want DATABASE_URL requirement", err)
	}
}
