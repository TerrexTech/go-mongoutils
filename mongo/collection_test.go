package mongo

import (
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MongoCollection", func() {
	type item struct {
		ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
		Word       string            `bson:"word" json:"word"`
		Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
		Hits       int               `bson:"hits,omitempty" json:"hits,omitempty"`
	}
	var connectionTimeout uint = 1000
	var resourceTimeout uint = 3000

	var c *Collection

	createTestCollection := func() *Collection {
		// collection to create
		collection := "test_collection"

		client, err := mgo.NewClient(connStr)
		Expect(err).ToNot(HaveOccurred())

		connCtx, connCancel := newTimeoutContext(connectionTimeout)
		err = client.Connect(connCtx)
		connCancel()
		Expect(err).ToNot(HaveOccurred())

		conn := &ConnectionConfig{
			Client:  client,
			Timeout: resourceTimeout,
		}
		c, err := EnsureCollection(&Collection{
			Connection:   conn,
			Database:     testDatabase,
			Name:         collection,
			SchemaStruct: &item{},
		})
		Expect(err).ToNot(HaveOccurred())
		return c
	}

	deleteTestDatabase := func() {
		client, err := mgo.NewClient(connStr)
		Expect(err).ToNot(HaveOccurred())

		connCtx, connCancel := newTimeoutContext(connectionTimeout)
		err = client.Connect(connCtx)
		connCancel()
		Expect(err).ToNot(HaveOccurred())

		dbCtx, dbCancel := newTimeoutContext(resourceTimeout)
		err = client.Database(testDatabase).Drop(dbCtx)
		dbCancel()
		Expect(err).ToNot(HaveOccurred())

		closeCtx, closeCancel := newTimeoutContext(connectionTimeout)
		defer closeCancel()
		err = client.Disconnect(closeCtx)
		Expect(err).ToNot(HaveOccurred())
	}

	BeforeEach(func() {
		deleteTestDatabase()
		c = createTestCollection()
	})

	AfterEach(func() {
		closeCtx, closeCancel := newTimeoutContext(connectionTimeout)
		defer closeCancel()
		err := c.Connection.Client.Disconnect(closeCtx)
		Expect(err).ToNot(HaveOccurred())
	})

	Describe("DeleteMany", func() {
		// Insert some test-data
		BeforeEach(func() {
			data1 := item{
				Word:       "some-word",
				Definition: "some-definition1",
			}
			_, err := c.InsertOne(data1)
			Expect(err).ToNot(HaveOccurred())
			data2 := item{
				Word:       "some-word",
				Definition: "some-definition2",
			}
			_, err = c.InsertOne(data2)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should delete any documents that match the filter", func() {
			result, err := c.DeleteMany(&item{
				Word: "some-word",
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(result.DeletedCount).To(Equal(int64(2)))
		})

		It("should throw error if filter-schema and collection-schema mismatch", func() {
			data := struct {
				Mismatch string
			}{
				Mismatch: "yup",
			}
			_, err := c.DeleteMany(data)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("Find", func() {
		// Insert some test-data
		BeforeEach(func() {
			data1 := item{
				Word:       "some-word",
				Definition: "some-definition1",
				Hits:       5,
			}
			_, err := c.InsertOne(data1)
			Expect(err).ToNot(HaveOccurred())

			data2 := item{
				Word:       "some-word2",
				Definition: "some-definition2",
				Hits:       8,
			}
			_, err = c.InsertOne(data2)
			Expect(err).ToNot(HaveOccurred())

			data3 := item{
				Word:       "some-word",
				Definition: "some-definition3",
				Hits:       8,
			}
			_, err = c.InsertOne(data3)
			Expect(err).ToNot(HaveOccurred())

			data4 := item{
				Word:       "some-word",
				Definition: "some-definition4",
				Hits:       10,
			}
			_, err = c.InsertOne(data4)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should find any documents that match the filter", func() {
			results, err := c.Find(&item{
				Word: "some-word",
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(len(results)).To(Equal(3))
			for _, r := range results {
				Expect(r.(*item).Word).To(Equal("some-word"))
			}
		})

		It(
			"should throw error if filter-schema and collection-schema mismatch",
			func() {
				data := struct {
					Mismatch string
				}{
					Mismatch: "yup",
				}
				_, err := c.DeleteMany(data)
				Expect(err).To(HaveOccurred())
			},
		)

		Describe("operations are performed on Find function", func() {
			Context("sort operation is performed", func() {
				It("should return results in asc order when asc is specified", func() {
					findResults, err := c.Find(
						&item{
							Word: "some-word",
						},
						findopt.Sort(
							map[string]interface{}{
								"hits": 1,
							},
						),
					)
					Expect(err).ToNot(HaveOccurred())

					hits := []int{}
					for _, r := range findResults {
						dbItem := r.(*item)
						hits = append(hits, dbItem.Hits)
					}

					Expect(len(hits)).To(Equal(3))
					Expect(hits[0]).To(Equal(5))
					Expect(hits[1]).To(Equal(8))
					Expect(hits[2]).To(Equal(10))
				})

				It("should return results in desc order when desc is specified", func() {
					findResults, err := c.Find(
						&item{
							Word: "some-word",
						},
						findopt.Sort(
							map[string]interface{}{
								"hits": -1,
							},
						),
					)
					Expect(err).ToNot(HaveOccurred())

					hits := []int{}
					for _, r := range findResults {
						dbItem := r.(*item)
						hits = append(hits, dbItem.Hits)
					}

					Expect(len(hits)).To(Equal(3))
					Expect(hits[0]).To(Equal(10))
					Expect(hits[1]).To(Equal(8))
					Expect(hits[2]).To(Equal(5))
				})
			})

			Context("limit is specified on top of sort operation", func() {
				It("should limit the \"find\" results as per limit", func() {
					findResults, err := c.Find(
						&item{
							Word: "some-word",
						},
						findopt.Sort(
							map[string]interface{}{
								"hits": -1,
							},
						),
						findopt.Limit(2),
					)
					Expect(err).ToNot(HaveOccurred())

					hits := []int{}
					for _, r := range findResults {
						dbItem := r.(*item)
						hits = append(hits, dbItem.Hits)
					}

					Expect(len(hits)).To(Equal(2))
					Expect(hits[0]).To(Equal(10))
					Expect(hits[1]).To(Equal(8))
				})
			})
		})
	})

	Describe("InsertOne", func() {
		It("should insert provided data if the data is not a pointer", func() {
			data1 := item{
				Word:       "some-word",
				Definition: "some-definition",
			}
			result, err := c.InsertOne(data1)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.InsertedID).To(BeAssignableToTypeOf(objectid.ObjectID{}))
		})

		It("should insert provided data if the data is a pointer", func() {
			data2 := &item{
				Word:       "some-word",
				Definition: "some-definition",
			}
			result, err := c.InsertOne(data2)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.InsertedID).To(BeAssignableToTypeOf(objectid.ObjectID{}))
		})

		It("should throw error if filter-schema and collection-schema mismatch", func() {
			data := struct {
				Mismatch string
			}{
				Mismatch: "yup",
			}
			_, err := c.InsertOne(data)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("UpdateMany", func() {
		// Insert some test-data
		BeforeEach(func() {
			data1 := item{
				Word:       "some-word",
				Definition: "some-definition1",
			}
			_, err := c.InsertOne(data1)
			Expect(err).ToNot(HaveOccurred())
			data2 := item{
				Word:       "some-word",
				Definition: "some-definition2",
			}
			_, err = c.InsertOne(data2)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should update the matching documents with provided data", func() {
			filter := item{
				Word: "some-word",
			}

			update := map[string]interface{}{
				"$set": map[string]interface{}{
					"definition": "some-definition1",
				},
			}

			result, err := c.UpdateMany(filter, update)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.MatchedCount).To(Equal(int64(2)))
			Expect(result.ModifiedCount).To(Equal(int64(1)))
		})

		It("should not upsert documents when no matches are found", func() {
			filter := item{
				Word: "invalid-stuff",
			}

			update := map[string]interface{}{
				"$set": map[string]interface{}{
					"definition": "some-definition1",
				},
			}

			result, err := c.UpdateMany(filter, update)
			Expect(err).ToNot(HaveOccurred())
			Expect(result.UpsertedID).To(BeNil())
		})
	})

	Describe("Aggregate", func() {
		It("should run the specified aggregate pipeline", func() {
			data1 := item{
				Word:       "some-word",
				Definition: "some-definition2",
				Hits:       5,
			}
			_, err := c.InsertOne(data1)
			Expect(err).ToNot(HaveOccurred())

			data2 := item{
				Word:       "some-word",
				Definition: "some-definition2",
				Hits:       10,
			}
			insertResult, err := c.InsertOne(data2)
			Expect(err).ToNot(HaveOccurred())

			pipeline := bson.NewArray(
				bson.VC.DocumentFromElements(
					bson.EC.SubDocumentFromElements(
						"$match",
						bson.EC.SubDocumentFromElements(
							"hits",
							bson.EC.Int32("$gt", 5),
						),
					),
				),
			)
			aggResults, err := c.Aggregate(pipeline)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(aggResults)).To(Equal(1))

			ar := aggResults[0].(*item)
			Expect(ar.ID).To(Equal(insertResult.InsertedID))
		})
	})
})
