### Go Mongo-Utils
---

This package helps in bootstrapping Mongo, including tasks such as creating
database-clients, databases, and collections.

**[Go Docs][0].**  
Check example usage here: [examples/example.go][1]  
More examples can be found in [test-files][2].

#### Developer Notes
---

Since the base driver, [mongo-go-driver][3], is in Alpha state, we run extensive
integration tests within required domain to ensure this library functions well.
Hence, a working MongoDB instance is required to run tests.

The tests will run using database `lib_test_db`, which **will be deleted** after
each test to ensure test-independence. So do not use this database for anything.

By default, the test-suite will try to read connection string from environment-variable
`MONGODB_TEST_CONN_STR`, however, if the variable is not present, the default string
`mongodb://root:root@localhost:27017` will be used for connection.

  [0]: https://godoc.org/github.com/TerrexTech/go-mongoutils/mongo
  [1]: https://github.com/TerrexTech/go-mongoutils/blob/master/examples/example.go
  [2]: https://github.com/TerrexTech/go-mongoutils/tree/master/mongo
  [3]: https://github.com/mongodb/mongo-go-driver
