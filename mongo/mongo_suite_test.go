package mongo

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var testDatabase = "lib_test_db"
var connStr = os.Getenv("MONGODB_TEST_CONN_STR")

// Tests will use this testDatabase
func TestCassandra(t *testing.T) {
	if connStr == "" {
		connStr = "mongodb://root:root@localhost:27017"
	}
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mongo Suite")
}
