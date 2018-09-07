package main

import (
	"context"
	"log"
	"time"

	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"

	"github.com/mongodb/mongo-go-driver/bson/objectid"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
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
	// ===========> MongoDB Client
	// Would ideally set connection string as an environment var
	client, err := mgo.NewClient("mongodb://root:root@localhost:27017")
	if err != nil {
		log.Fatalln(err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(1)*time.Second,
	)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	// ===========> Collection Configuration
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

	// ===========> Create New Collection
	c := &mongo.Collection{
		Connection:   conn,
		Name:         "test_coll",
		Database:     "test",
		SchemaStruct: &item{},
		Indexes:      indexConfigs,
	}
	col, err := mongo.EnsureCollection(c)
	if err != nil {
		log.Fatalln(err)
	}

	// ===========> Insert Data
	data1 := &item{
		Word:       "some-word",
		Definition: "some-definition",
		Hits:       5,
	}
	insertResult, err := c.InsertOne(data1)
	if err != nil {
		err = errors.Wrap(
			err,
			"Error Inserting Data into Collection. "+
				"Most likely, this is because the same data has already been inserted, "+
				"so the file-execution will still continue",
		)
		log.Println(err)
	}
	log.Println("Insert Result 1:")
	log.Println(insertResult)

	// Another Data Insertion
	data2 := &item{
		Word:       "some-word2",
		Definition: "some-definition",
		Hits:       3,
	}
	insertResult, err = c.InsertOne(data2)
	if err != nil {
		err = errors.Wrap(
			err,
			"Error Inserting Data into Collection. "+
				"Most likely, this is because the same data has already been inserted, "+
				"so the file-execution will still continue",
		)
		log.Println(err)
	}
	log.Println("Insert Result 2:")
	log.Println(insertResult)

	// And yet another Data Insertion
	data3 := &item{
		Word:       "some-word3",
		Definition: "some-definition",
		Hits:       7,
	}
	insertResult, err = c.InsertOne(data3)
	if err != nil {
		err = errors.Wrap(
			err,
			"Error Inserting Data into Collection. "+
				"Most likely, this is because the same data has already been inserted, "+
				"so the file-execution will still continue",
		)
		log.Println(err)
	}
	log.Println("Insert Result 3:")
	log.Println(insertResult)

	// ===========> Find Data
	log.Println("=========================")
	// Filter for our data
	findResults, err := c.Find(&item{
		Word: "some-word",
	})
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Find Results:")
	for _, r := range findResults {
		dbItem := r.(*item)
		log.Println(dbItem.Word, dbItem.Definition)
	}

	// ===========> Find Data - Sort ASC
	log.Println("=========================")
	findResults, err = c.Find(
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

	// ===========> Find Data - Get Max (Limit DESC Sort to 1)
	log.Println("=========================")
	findResults, err = c.Find(
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

	log.Println("Find Results (sorted in ascending order of \"Hits\"):")
	for _, r := range findResults {
		dbItem := r.(*item)
		log.Printf("%+v\n", dbItem)
	}

	// ===========> Update Data
	filter := &item{
		Word: "some-word",
	}
	update := &map[string]interface{}{
		"$set": map[string]interface{}{
			"definition": "updated-definition",
		},
	}
	updateResult, err := col.UpdateMany(filter, update)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Update Result:")
	log.Println(updateResult)

	// ===========> Delete Data
	delResult, err := c.DeleteMany(&item{
		Word: "some-word",
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Delete Result:")
	log.Println(delResult)
}
