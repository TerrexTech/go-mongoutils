package mongo

import (
	"reflect"
	"strings"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	"github.com/pkg/errors"

	mgo "github.com/mongodb/mongo-go-driver/mongo"
)

// ConnectionConfig defines the Client to use for
// communicating with MongoDB, and the Timeout for that client.
type ConnectionConfig struct {
	Client  *Client
	Timeout uint32
}

// IndexColumnConfig defines configuration for
// a column in index-definition.
type IndexColumnConfig struct {
	Name        string
	IsDescOrder bool
}

// IndexConfig defines configuration for indexes to be created
// when creating this collection.
type IndexConfig struct {
	ColumnConfig []IndexColumnConfig
	IsUnique     bool
	Name         string
}

// Collection represents the MongoDB collection.
type Collection struct {
	Connection *ConnectionConfig
	Database   string
	Name       string
	// Indexes to be created when creating collection
	Indexes      []IndexConfig
	SchemaStruct interface{}
	collection   *mgo.Collection
}

// verifyDataSchema checks if the provided data's schema matches the
// Collection.SchemaStruct. The SchemaStruct can be changed as required,
// this is only intended to prevent unexpected behavior.
func (c *Collection) verifyDataSchema(data interface{}) error {
	dataType := reflect.TypeOf(data).String()
	// Add "*" if absent because the SchemaStruct we stored is a pointer
	if !strings.HasPrefix(dataType, "*") {
		dataType = "*" + dataType
	}
	expectedType := reflect.TypeOf(c.SchemaStruct).String()

	if dataType != expectedType {
		return errors.New(
			"Mismatch between provided data and expected schema." +
				"Consider changing collection.SchemaStruct if required.",
		)
	}
	return nil
}

// DeleteMany deletes multiple documents from the collection.
// The filter-data must match the schema provided at the time of Collection-
// creation. Update the Collection.SchemaStruct if new schema is required.
func (c *Collection) DeleteMany(filter interface{}) (*mgo.DeleteResult, error) {
	err := c.verifyDataSchema(filter)
	if err != nil {
		return nil, errors.Wrap(err, "DeleteMany - Schema Verification Error")
	}
	doc, err := toBSON(filter)
	if err != nil {
		return nil, errors.Wrap(err, "DeleteMany - BSON Convert Error")
	}

	ctx, cancel := newTimeoutContext(c.Connection.Timeout)
	defer cancel()

	result, err := c.collection.DeleteMany(ctx, doc)
	if err != nil {
		cancel()
	}
	return result, errors.Wrap(err, "Deletion Error")
}

// Find finds the documents matching a model.
// The filter-data must match the schema provided at the time of Collection-
// creation. Update the Collection.SchemaStruct if new schema is required.
func (c *Collection) Find(
	filter interface{},
	opts ...findopt.Find,
) ([]interface{}, error) {
	err := c.verifyDataSchema(filter)
	if err != nil {
		return nil, errors.Wrap(err, "Find - Schema Verification Error")
	}
	doc, err := toBSON(filter)
	if err != nil {
		return nil, errors.Wrap(err, "Find - BSON Convert Error")
	}

	findCtx, findCancel := newTimeoutContext(c.Connection.Timeout)
	cur, err := c.collection.Find(findCtx, doc, opts...)
	if err != nil {
		findCancel()
		return nil, errors.Wrap(err, "Find Error")
	}
	findCancel()

	items := make([]interface{}, 0)
	cursorCtx, cursorCancel := newTimeoutContext(c.Connection.Timeout)
	for cur.Next(cursorCtx) {
		item := copyInterface(c.SchemaStruct)
		err := cur.Decode(item)
		if err != nil {
			cursorCancel()
			return nil, errors.Wrap(err, "Find - Cursor Decode Error")
		}
		items = append(items, item)
	}
	cursorCancel()

	cursorCloseCtx, cursorCloseCancel := newTimeoutContext(c.Connection.Timeout)
	defer cursorCloseCancel()
	err = cur.Close(cursorCloseCtx)
	if err != nil {
		err = errors.Wrap(err, "Find - Error Closing Cursor")
	}
	return items, err
}

// InsertOne inserts the provided data into Collection.
// The data must match the schema provided at the time of Collection-
// creation. Update the Collection.SchemaStruct if new schema is required.
func (c *Collection) InsertOne(data interface{}) (*mgo.InsertOneResult, error) {
	err := c.verifyDataSchema(data)
	if err != nil {
		return nil, errors.Wrap(err, "InsertOne - Schema Verification Error")
	}
	doc, err := toBSON(data)
	if err != nil {
		return nil, errors.Wrap(err, "InsertOne - BSON Convert Error")
	}

	ctx, cancel := newTimeoutContext(c.Connection.Timeout)
	defer cancel()

	result, err := c.collection.InsertOne(ctx, doc)
	if err != nil {
		cancel()
	}
	return result, errors.Wrap(err, "InsertOne Error")
}

// UpdateMany updates multiple documents in the collection.
// A map or a struct can be supplied as filter-data or update-data,
// both are transformed into BSON using bson#NewDocumentEncoder#EncodeDocument.
func (c *Collection) UpdateMany(
	filter interface{},
	update interface{},
) (*mgo.UpdateResult, error) {
	// We don't verify schema here to allow flexibility in
	// update-methods. This might change in future as per requirements.
	updateDoc, err := toBSON(update)
	if err != nil {
		return nil, errors.Wrap(err, "UpdateMany - BSON Convert Error")
	}
	filterDoc, err := toBSON(filter)
	if err != nil {
		return nil, errors.Wrap(err, "UpdateMany - BSON Convert Error")
	}

	ctx, cancel := newTimeoutContext(c.Connection.Timeout)
	defer cancel()

	result, err := c.collection.UpdateMany(ctx, filterDoc, updateDoc)
	if err != nil {
		cancel()
	}
	return result, errors.Wrap(err, "UpdateMany Error")
}

// Aggregate runs an aggregation framework pipeline
// See https://docs.mongodb.com/manual/aggregation/.
func (c *Collection) Aggregate(pipeline interface{}) ([]interface{}, error) {
	aggCtx, aggCancel := newTimeoutContext(c.Connection.Timeout)
	cur, err := c.collection.Aggregate(aggCtx, pipeline)
	aggCancel()

	if err != nil {
		err = errors.Wrap(err, "Aggregate Error")
		return nil, err
	}

	items := make([]interface{}, 0)
	curCtx, curCancel := newTimeoutContext(c.Connection.Timeout)
	for cur.Next(curCtx) {
		item := copyInterface(c.SchemaStruct)
		err := cur.Decode(item)
		if err != nil {
			curCancel()
			return nil, errors.Wrap(err, "Aggregate - Cursor Decode Error")
		}
		items = append(items, item)
	}
	curCancel()

	cursorCloseCtx, cursorCloseCancel := newTimeoutContext(c.Connection.Timeout)
	defer cursorCloseCancel()
	err = cur.Close(cursorCloseCtx)
	if err != nil {
		err = errors.Wrap(err, "Aggregate - Error Closing Cursor")
	}
	return items, err
}
