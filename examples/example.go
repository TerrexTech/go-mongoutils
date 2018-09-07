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
	collection, err := createCollection()
	if err != nil {
		log.Fatalln(err, collection)
	}

	// ====> Insert Data
	data1 := &item{
		Word:       "some-word",
		Definition: "some-definition",
		Hits:       3,
	}
	insertData(collection, data1)

	// Insert another record
	data2 := &item{
		Word:       "some-word2",
		Definition: "some-definition",
		Hits:       7,
	}
	insertData(collection, data2)

	// ====> Find Data
	findData(collection)

	// ====> Find data and sort it by "Hits" is ascending order
	findDataAndSortByHitsAsc(collection)

	// ====> Get Max number of "Hits"
	getMaxHits(collection)

	// ====> Update Data
	update(collection)

	// ====> Delete Data
	delete(collection)

	// ====> Aggregate Pipeline
	aggregatePipeline(collection)
}

// createCollection demonstrates creating the collection and the associated database.
func createCollection() (*mongo.Collection, error) {
	// Would ideally set these config-params as environment vars
	config := mongo.ClientConfig{
		Hosts:               []string{"localhost:27017"},
		Username:            "root",
		Password:            "root",
		TimeoutMilliseconds: 3000,
	}

	// ====> MongoDB Client
	client, err := mongo.NewClient(config)
	if err != nil {
		log.Fatalln(err)
	}

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
func insertData(collection *mongo.Collection, data interface{}) {
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

// update demonstrates updating an existing record.
func update(collection *mongo.Collection) {
	filter := &item{
		Word: "some-word",
	}
	update := &map[string]interface{}{
		"$set": map[string]interface{}{
			"definition": "updated-definition",
		},
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
