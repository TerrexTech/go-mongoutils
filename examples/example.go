package main

import (
	"log"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"github.com/pkg/errors"
)

// This is a type to hold our word definitions in
// we specifiy both bson (for MongoDB) and json (for web)
// naming for marshalling and unmarshalling
type item struct {
	ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Word       string            `bson:"word,omitempty" json:"word,omitempty"`
	Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
	Hits       int               `bson:"hits,omitempty" json:"hits,omitempty"`
}

func main() {
	log.Println("==========> Demonstrate Client Creation")
	client, err := createClient()
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("==========> Demonstrate Collection Creation")
	collection, err := createCollection(client)
	if err != nil {
		log.Fatalln(err, collection)
	}

	// ====> Insert Data
	log.Println("==========> Demonstrate Data Insertion")
	insertData(collection)

	// ====> Insert Many
	insertManyData(collection)

	// ====> Find Data
	log.Println("==========> Demonstrate Finding Data")
	findData(collection)

	// ====> Find data and sort it by "Hits" is ascending order
	log.Println("==========> Demonstrate Finding and Sorting Data")
	findDataAndSortByHitsAsc(collection)

	// ====> Get Max number of "Hits"
	log.Println("==========> Demonstrate Get Max from Documents")
	getMaxHits(collection)

	// ====> Get Values betweeen two Numbers
	log.Println("==========> Demonstrate getting Data in Range")
	getValuesInRange(collection)

	// ====> Update Data
	log.Println("==========> Demonstrate Updating Data")
	update(collection)

	// ====> Delete Data
	log.Println("==========> Demonstrate Deleting Data")
	delete(collection)

	// ====> Aggregate Pipeline
	log.Println("==========> Demonstrate Aggregate Pipeline")
	aggregatePipeline(collection)
}

// createClient creates a MongoDB-Client.
func createClient() (*mongo.Client, error) {
	// Would ideally set these config-params as environment vars
	config := mongo.ClientConfig{
		Hosts:               []string{"localhost:27017"},
		Username:            "root",
		Password:            "root",
		TimeoutMilliseconds: 3000,
	}

	// ====> MongoDB Client
	client, err := mongo.NewClient(config)
	// Let the parent functions handle error, always -.-
	// (Even though in these examples, we won't, for simplicity)
	return client, err
}

// createCollection demonstrates creating the collection and the associated database.
func createCollection(client *mongo.Client) (*mongo.Collection, error) {
	// ====> Collection Configuration
	conn := &mongo.ConnectionConfig{
		Client:  client,
		Timeout: 5000,
	}
	// Index Configuration
	indexConfigs := []mongo.IndexConfig{
		mongo.IndexConfig{
			ColumnConfig: []mongo.IndexColumnConfig{
				mongo.IndexColumnConfig{
					Name:        "word",
					IsDescOrder: true,
				},
			},
			IsUnique: true,
			Name:     "test_index",
		},
	}

	// ====> Create New Collection
	c := &mongo.Collection{
		Connection:   conn,
		Name:         "test_coll",
		Database:     "test",
		SchemaStruct: &item{},
		Indexes:      indexConfigs,
	}
	return mongo.EnsureCollection(c)
}

// insertData demonstrates inserting specified data into collection.
func insertData(collection *mongo.Collection) {
	data := &item{
		Word:       "some-word",
		Definition: "some-definition",
		Hits:       3,
	}

	insertResult, err := collection.InsertOne(data)
	if err != nil {
		err = errors.Wrap(
			err,
			"Error Inserting Data into Collection. "+
				"Most likely, this is because the same data has already been inserted, "+
				"so the file-execution will still continue",
		)
		log.Println(err)
	}
	log.Println("Insert Result:")
	log.Println(insertResult)
}

func insertManyData(collection *mongo.Collection) {
	log.Println("==========> Demonstrate \"Many\" (array/slice) Data Insertion")
	manyData := []interface{}{
		&item{
			Word:       "some-word",
			Definition: "some-definition",
			Hits:       5,
		},
		&item{
			Word:       "some-word2",
			Definition: "some-definition",
			Hits:       7,
		},
		&item{
			Word:       "some-word",
			Definition: "some-definition",
			Hits:       8,
		},
		&item{
			Word:       "some-word",
			Definition: "some-definition",
			Hits:       10,
		},
	}
	manyResult, err := collection.InsertMany(manyData)
	if err != nil {
		err = errors.Wrap(
			err,
			"Error in InsertMany. "+
				"Most likely, this is because the same data has already been inserted, "+
				"so the file-execution will still continue. "+
				"However, no more insertions will be made from same data-set",
		)
		log.Println(err)
	}
	log.Println(manyResult)
}

// findData demostrates finding the data from collection.
func findData(collection *mongo.Collection) {
	// Filter for our data
	findResults, err := collection.Find(&item{
		Definition: "some-definition",
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Find Results:")
	for _, r := range findResults {
		dbItem := r.(*item)
		log.Printf("%+v\n", dbItem)
	}
}

// findDataAndSortByHitsAsc demonstrates using "Sort" with "Find".
func findDataAndSortByHitsAsc(collection *mongo.Collection) {
	// Filter for our data
	findResults, err := collection.Find(
		// The filter parameter
		&item{
			Definition: "some-definition",
		},
		// Sorting the result.
		// 1 = Ascending Order (default)
		// -1 = Descending Order
		findopt.Sort(
			map[string]interface{}{
				"hits": 1,
			},
		),
	)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Find Results (sorted in ascending order of \"Hits\"):")
	for _, r := range findResults {
		dbItem := r.(*item)
		log.Printf("%+v\n", dbItem)
	}
}

// getMaxHits demonstrates getting a "Max" value from collection.
// In this case, we are getting max values of field "Hits".
func getMaxHits(collection *mongo.Collection) {
	// Filter for our data
	findResults, err := collection.Find(
		// The filter parameter
		&item{
			Definition: "some-definition",
		},
		// Sorting the result.
		// 1 = Ascending Order (default)
		// -1 = Descending Order
		findopt.Sort(
			map[string]interface{}{
				"hits": -1,
			},
		),
		// We only want the maximum value
		findopt.Limit(1),
	)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Find Results (max value of \"Hits\"):")
	for _, r := range findResults {
		dbItem := r.(*item)
		// Can do dbItem.Hits to just display "Hits"
		log.Printf("%+v\n", dbItem)
	}
}

// getValuesInRange demonstrates getting values between two numbers.
func getValuesInRange(collection *mongo.Collection) {
	results, err := collection.FindMap(map[string]interface{}{
		"hits": map[string]int{
			"$gt": 4,
			"$lt": 9,
		},
	})

	if err != nil {
		log.Fatalln(err)
	}
	log.Println(results)
}

// update demonstrates updating an existing record.
func update(collection *mongo.Collection) {
	filter := &item{
		Word: "some-word",
	}
	// Key: NewValue
	update := &map[string]interface{}{
		"definition": "updated-definition",
	}
	updateResult, err := collection.UpdateMany(filter, update)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Update Result:")
	log.Println(updateResult)
}

// delete demonstrates deleting an existing record.
func delete(collection *mongo.Collection) {
	delResult, err := collection.DeleteMany(&item{
		Word: "some-word",
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Delete Result:")
	log.Println(delResult)
}

// aggregatePipeline demonstrates using mongo-pipelines.
func aggregatePipeline(collection *mongo.Collection) {
	pipeline := bson.NewArray(
		bson.VC.DocumentFromElements(
			bson.EC.SubDocumentFromElements(
				"$match",
				bson.EC.SubDocumentFromElements(
					"hits",
					bson.EC.Int32("$gte", 2),
				),
			),
		),
	)
	aggResults, err := collection.Aggregate(pipeline)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(len(aggResults))
	for _, r := range aggResults {
		log.Println(r.(*item))
	}
}
