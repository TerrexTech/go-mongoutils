package mongo

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/TerrexTech/go-commonutils/utils"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

var _ = Describe("Mongo - NewCollection", func() {
	Context("new collection is created", func() {
		var (
			connectionTimeout uint32
			resourceTimeout   uint32
			clientConfig      ClientConfig
			testDatabase      string
		)

		type item struct {
			ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
			Word       string            `bson:"word" json:"word"`
			Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
		}

		// Drop the test-database
		BeforeEach(func() {
			hosts := os.Getenv("MONGO_TEST_HOSTS")
			username := os.Getenv("MONGO_TEST_USERNAME")
			password := os.Getenv("MONGO_TEST_PASSWORD")
			connectionTimeoutStr := os.Getenv("MONGO_TEST_CONNECTION_TIMEOUT_MS")
			resourceTimeoutStr := os.Getenv("MONGO_TEST_RESOURCE_TIMEOUT_MS")
			testDatabase = os.Getenv("MONGO_TEST_DATABASE")

			var err error
			// Set Connection Timeout
			connectionTimeoutInt, err := strconv.Atoi(connectionTimeoutStr)
			if err != nil {
				err = errors.Wrap(
					err,
					"error getting CONNECTION_TIMEOUT from env, will use 1000",
				)
				log.Println(err)
				connectionTimeoutInt = 1000
			}
			connectionTimeout = uint32(connectionTimeoutInt)

			// Set Resource Timeout
			resourceTimeoutInt, err := strconv.Atoi(resourceTimeoutStr)
			if err != nil {
				err = errors.Wrap(
					err,
					"error getting RESOURCE_TIMEOUT from env, will use 1000",
				)
				log.Println(err)
				resourceTimeoutInt = 3000
			}
			resourceTimeout = uint32(resourceTimeoutInt)

			// Client Configuration
			clientConfig = ClientConfig{
				Hosts:               *utils.ParseHosts(hosts),
				Username:            username,
				Password:            password,
				TimeoutMilliseconds: connectionTimeout,
			}

			// Create a Mongo Client
			client, err := NewClient(clientConfig)
			Expect(err).ToNot(HaveOccurred())

			dbCtx, dbCancel := newTimeoutContext(resourceTimeout)
			err = client.Database(testDatabase).Drop(dbCtx)
			dbCancel()
			Expect(err).ToNot(HaveOccurred())

			err = client.Disconnect()
			Expect(err).ToNot(HaveOccurred())
		})

		It("should return error if Collection arguments is nil", func() {
			_, err := EnsureCollection(nil)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if SchemaStruct is not specified", func() {
			_, err := EnsureCollection(&Collection{})
			Expect(err).To(HaveOccurred())
		})

		It("should return error if SchemaStruct is not a pointer to struct", func() {
			type item struct{}
			c := &Collection{
				Name:         "test_coll",
				Database:     "test",
				SchemaStruct: item{},
				Indexes:      nil,
			}
			_, err := EnsureCollection(c)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if SchemaStruct is a pointer to non-struct", func() {
			c := &Collection{
				Name:         "test_coll",
				Database:     "test",
				SchemaStruct: &[]string{},
				Indexes:      nil,
			}
			_, err := EnsureCollection(c)
			Expect(err).To(HaveOccurred())
		})

		It("should return error if invalid index keys are specified", func() {
			indexConfigs := []IndexConfig{
				IndexConfig{
					ColumnConfig: []IndexColumnConfig{
						IndexColumnConfig{
							Name:        "invalid-column",
							IsDescOrder: true,
						},
					},
					IsUnique: true,
					Name:     "test_index",
				},
			}

			c := &Collection{
				Name:         "test_coll",
				Database:     "test",
				SchemaStruct: &item{},
				Indexes:      indexConfigs,
			}

			_, err := EnsureCollection(c)
			Expect(err).To(HaveOccurred())
		})

		It("should create database as specified", func() {
			// Collection to create
			collection := "test_collection"

			client, err := NewClient(clientConfig)
			Expect(err).ToNot(HaveOccurred())

			err = client.Connect()
			Expect(err).ToNot(HaveOccurred())

			// Create collection and database
			indexConfigs := []IndexConfig{
				IndexConfig{
					ColumnConfig: []IndexColumnConfig{
						IndexColumnConfig{
							Name:        "word",
							IsDescOrder: true,
						},
					},
					IsUnique: true,
					Name:     "test_index",
				},
			}
			conn := &ConnectionConfig{
				Client:  client,
				Timeout: resourceTimeout,
			}
			c := &Collection{
				Connection:   conn,
				Database:     testDatabase,
				Indexes:      indexConfigs,
				Name:         collection,
				SchemaStruct: &item{},
			}
			_, err = EnsureCollection(c)
			Expect(err).ToNot(HaveOccurred())

			// Get Cursor to collections in database
			collCtx, collCancel := newTimeoutContext(connectionTimeout)
			cur, err := client.Database(testDatabase).ListCollections(collCtx, nil)
			collCancel()
			Expect(err).ToNot(HaveOccurred())

			// Iterate over collections in the database
			curCtx, curCancel := newTimeoutContext(resourceTimeout)
			var result string
			for cur.Next(curCtx) {
				next := bson.NewDocument()
				err = cur.Decode(next)
				Expect(err).ToNot(HaveOccurred())
				result = next.Lookup("name").StringValue()
			}
			curCancel()
			Expect(strings.Contains(result, collection)).To(BeTrue())

			cursorCloseCtx, cursorCloseCancel := newTimeoutContext(c.Connection.Timeout)
			defer cursorCloseCancel()
			err = cur.Close(cursorCloseCtx)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should create indexes as specified", func() {
			client, err := NewClient(clientConfig)
			Expect(err).ToNot(HaveOccurred())

			err = client.Connect()
			Expect(err).ToNot(HaveOccurred())

			type item struct {
				ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
				Word       string            `bson:"word" json:"word"`
				Definition string            `bson:"definition" json:"definition"`
			}
			indexConfigs := []IndexConfig{
				IndexConfig{
					ColumnConfig: []IndexColumnConfig{
						IndexColumnConfig{
							Name:        "word",
							IsDescOrder: true,
						},
						IndexColumnConfig{
							Name: "definition",
						},
					},
					IsUnique: true,
					Name:     "test_index1",
				},
				IndexConfig{
					ColumnConfig: []IndexColumnConfig{
						IndexColumnConfig{
							Name: "definition",
						},
					},
					IsUnique: false,
					Name:     "test_index2",
				},
			}
			conn := &ConnectionConfig{
				Client:  client,
				Timeout: resourceTimeout,
			}

			c, err := EnsureCollection(&Collection{
				Connection:   conn,
				Database:     testDatabase,
				Indexes:      indexConfigs,
				Name:         "test_collection",
				SchemaStruct: &item{},
			})
			Expect(err).ToNot(HaveOccurred())

			// Get indexes
			indexCtx, indexCancel := newTimeoutContext(connectionTimeout)
			cur, err := c.collection.Indexes().List(indexCtx)
			indexCancel()
			Expect(err).ToNot(HaveOccurred())

			curCtx, curCancel := newTimeoutContext(resourceTimeout)

			// Create Document-Array from indexes
			indexDocs := map[string]*bson.Document{}
			for cur.Next(curCtx) {
				next := bson.NewDocument()
				err = cur.Decode(next)
				Expect(err).ToNot(HaveOccurred())

				indexName := next.Lookup("name").StringValue()
				indexDocs[indexName] = next
			}
			curCancel()

			cursorCloseCtx, cursorCloseCancel := newTimeoutContext(c.Connection.Timeout)
			err = cur.Close(cursorCloseCtx)
			cursorCloseCancel()
			Expect(err).ToNot(HaveOccurred())

			// ====> Inspect index "test_index1"
			testIndex1 := indexDocs["test_index1"]
			// It should belong to collection "lib_test_db.test_collection"
			Expect(
				testIndex1.Lookup("ns").StringValue(),
			).To(Equal("lib_test_db.test_collection"))
			// It should have uniqueness
			Expect(testIndex1.Lookup("unique").Boolean()).To(BeTrue())
			// It should have keys: "word" and "definition"
			keys := testIndex1.LookupElement("key")
			keysDoc := keys.Value().MutableDocument()
			// The index-key "word" has to be in descending order
			Expect(
				keysDoc.Lookup("word").Int32(),
			).To(Equal(int32(-1)))
			// The index-key "definition" has to be in ascending order
			Expect(
				keysDoc.Lookup("definition").Int32(),
			).To(Equal(int32(1)))

			// ====> Inspect index "test_index2"
			testIndex2 := indexDocs["test_index2"]
			// It should belong to collection "lib_test_db.test_collection"
			Expect(
				testIndex1.Lookup("ns").StringValue(),
			).To(Equal("lib_test_db.test_collection"))
			// It should not have uniqueness
			Expect(testIndex2.Lookup("unique")).To(BeNil())
			// It should have key: "definition"
			keys = testIndex1.LookupElement("key")
			keysDoc = keys.Value().MutableDocument()
			// The index-key "definition" has to be in ascending order
			Expect(
				keysDoc.Lookup("definition").Int32(),
			).To(Equal(int32(1)))
		})

		It(
			"should pass index verification even if the key includes 'omitempty' in tag",
			func() {
				client, err := NewClient(clientConfig)
				Expect(err).ToNot(HaveOccurred())

				err = client.Connect()
				Expect(err).ToNot(HaveOccurred())

				type item struct {
					ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
					Word       string            `bson:"word,omitempty" json:"word"`
					Definition string            `bson:"definition" json:"definition"`
				}
				indexConfigs := []IndexConfig{
					IndexConfig{
						ColumnConfig: []IndexColumnConfig{
							IndexColumnConfig{
								Name:        "word",
								IsDescOrder: true,
							},
						},
						IsUnique: true,
						Name:     "test_index1",
					},
				}
				conn := &ConnectionConfig{
					Client:  client,
					Timeout: resourceTimeout,
				}

				c, err := EnsureCollection(&Collection{
					Connection:   conn,
					Database:     testDatabase,
					Indexes:      indexConfigs,
					Name:         "test_collection",
					SchemaStruct: &item{},
				})
				Expect(err).ToNot(HaveOccurred())

				// Get indexes
				indexCtx, indexCancel := newTimeoutContext(connectionTimeout)
				cur, err := c.collection.Indexes().List(indexCtx)
				indexCancel()
				Expect(err).ToNot(HaveOccurred())

				curCtx, curCancel := newTimeoutContext(resourceTimeout)

				// Create Document-Array from indexes
				indexDocs := map[string]*bson.Document{}
				for cur.Next(curCtx) {
					next := bson.NewDocument()
					err = cur.Decode(next)
					Expect(err).ToNot(HaveOccurred())

					indexName := next.Lookup("name").StringValue()
					indexDocs[indexName] = next
				}
				curCancel()

				cursorCloseCtx, cursorCloseCancel := newTimeoutContext(c.Connection.Timeout)
				err = cur.Close(cursorCloseCtx)
				cursorCloseCancel()
				Expect(err).ToNot(HaveOccurred())

				// ====> Inspect index "test_index1"
				testIndex1 := indexDocs["test_index1"]
				// It should belong to collection "lib_test_db.test_collection"
				Expect(
					testIndex1.Lookup("ns").StringValue(),
				).To(Equal("lib_test_db.test_collection"))
				// It should have uniqueness
				Expect(testIndex1.Lookup("unique").Boolean()).To(BeTrue())
				// It should have the key: "word"
				keys := testIndex1.LookupElement("key")
				keysDoc := keys.Value().MutableDocument()
				// The index-key "word" has to be in descending order
				Expect(
					keysDoc.Lookup("word").Int32(),
				).To(Equal(int32(-1)))
			},
		)

		It("should timeout on invalid connection-string", func() {
			clientConfig.Hosts = []string{"invalid-conn-str"}
			client, err := NewClient(clientConfig)
			Expect(err).ToNot(HaveOccurred())

			indexConfigs := []IndexConfig{
				IndexConfig{
					ColumnConfig: []IndexColumnConfig{
						IndexColumnConfig{
							Name:        "word",
							IsDescOrder: true,
						},
					},
					IsUnique: true,
					Name:     "test_index",
				},
			}
			conn := &ConnectionConfig{
				Client:  client,
				Timeout: connectionTimeout,
			}

			c := &Collection{
				Connection:   conn,
				Database:     testDatabase,
				Indexes:      indexConfigs,
				Name:         "test_collection",
				SchemaStruct: &item{},
			}

			_, err = EnsureCollection(c)
			Expect(err).To(HaveOccurred())
		})
	})
})
