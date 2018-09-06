package main

import (
	"context"
	"log"
	"time"

	"github.com/TerrexTech/go-mongoutils/mongo"

	"github.com/mongodb/mongo-go-driver/bson/objectid"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
)

// This is a type to hold our word definitions in
// we specifiy both bson (for MongoDB) and json (for web)
// naming for marshalling and unmarshalling
type item struct {
	ID         objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
	Word       string            `bson:"word" json:"word"`
	Definition string            `bson:"definition,omitempty" json:"definition,omitempty"`
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
	data := &item{
		Word:       "some-word",
		Definition: "some-definition",
	}
	insertResult, err := c.InsertOne(data)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Insert Result:")
	log.Println(insertResult)

	// ===========> Find Data
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
