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
	// Add "*" if absent because the SchemaStruct we stored is a pointer,
	// but its not necessary that data (to validate) must also be a pointer
	// (for example, data-insertion can be used with and without a pointer),
	if !strings.HasPrefix(dataType, "*") {
		dataType = "*" + dataType
	}
	expectedType := reflect.TypeOf(c.SchemaStruct).String()

	if dataType != expectedType {
		return errors.New(
			"Mismatch between provided data-schema and expected schema. " +
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
		err = errors.Wrap(err, "Deletion Error")
	}
	return result, err
}

// Find finds the documents matching the filter.
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

// FindOne returns single result that matches the provided filter.
// The filter-data must match the schema provided at the time of Collection-
// creation. Update the Collection.SchemaStruct if new schema is required.
func (c *Collection) FindOne(
	filter interface{},
	opts ...findopt.One,
) (interface{}, error) {
	err := c.verifyDataSchema(filter)
	if err != nil {
		return nil, errors.Wrap(err, "Find - Schema Verification Error")
	}
	doc, err := toBSON(filter)
	if err != nil {
		return nil, errors.Wrap(err, "Find - BSON Convert Error")
	}

	findCtx, findCancel := newTimeoutContext(c.Connection.Timeout)

	result := copyInterface(c.SchemaStruct)
	err = c.collection.FindOne(findCtx, doc, opts...).Decode(result)
	if err != nil {
		findCancel()
		return nil, errors.Wrap(err, "FindOne Decoding Error")
	}
	findCancel()

	return result, nil
}

// FindMap finds the documents matching the filter.
// The filter-data must be a map analogous to how the "find" argument would be
// used in MongoDB. For example, a find-query in MongoDB such as:
//  {hits: {$gt: 4, $lt: 9}}
// can be represented in a map as:
//  map[string]interface{}{
//	  "hits": map[string]interface{
//      "$gt": 4,
//      "$lt": 9,
//    },
//  }
func (c *Collection) FindMap(
	filter interface{},
	opts ...findopt.Find,
) ([]interface{}, error) {
	if reflect.TypeOf(filter).Kind() == reflect.Ptr {
		return nil, errors.New("FindMap - Filter must be a non-pointer map")
	}

	isValidFilter := verifyKind(filter, reflect.Map)
	if !isValidFilter {
		return nil, errors.New(
			"FindMap - Data must be a Map (pointer or non-pointer)",
		)
	}

	findCtx, findCancel := newTimeoutContext(c.Connection.Timeout)
	cur, err := c.collection.Find(findCtx, filter, opts...)
	if err != nil {
		findCancel()
		return nil, errors.Wrap(err, "FindMap Error")
	}
	findCancel()

	items := make([]interface{}, 0)
	cursorCtx, cursorCancel := newTimeoutContext(c.Connection.Timeout)
	for cur.Next(cursorCtx) {
		item := copyInterface(c.SchemaStruct)
		err := cur.Decode(item)
		if err != nil {
			cursorCancel()
			return nil, errors.Wrap(err, "FindMap - Cursor Decode Error")
		}
		items = append(items, item)
	}
	cursorCancel()

	cursorCloseCtx, cursorCloseCancel := newTimeoutContext(c.Connection.Timeout)
	defer cursorCloseCancel()
	err = cur.Close(cursorCloseCtx)
	if err != nil {
		err = errors.Wrap(err, "FindMap - Error Closing Cursor")
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
		err = errors.Wrap(err, "InsertOne Error")
	}
	return result, err
}

// InsertMany inserts the provided data into Collection.
// Currently, batching is not implemented for this operation.
// Because of this, extremely large sets of documents will not fit into a
// single BSON document to be sent to the server, so the operation will fail.
// The data must match the schema provided at the time of Collection-
// creation. Update the Collection.SchemaStruct if new schema is required.
func (c *Collection) InsertMany(
	data []interface{},
) (*[]mgo.InsertOneResult, error) {
	isValidData := verifyKind(data, reflect.Array, reflect.Slice)
	if !isValidData {
		return nil, errors.New(
			"InsertMany - Data must be Array or Slice (pointer or non-pointer)",
		)
	}

	insertResults := []mgo.InsertOneResult{}
	for i, d := range data {
		result, err := c.InsertOne(d)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"InsertMany - Error Inserting Data at Index: %d", i,
			)
		}
		insertResults = append(insertResults, *result)
	}
	return &insertResults, nil
}

// UpdateMany updates multiple documents in the collection.
// A map or a struct can be supplied as filter-data or update-data,
// both are transformed into BSON using bson#NewDocumentEncoder#EncodeDocument.
func (c *Collection) UpdateMany(
	filter interface{},
	update interface{},
) (*mgo.UpdateResult, error) {
	isValidFilter := verifyKind(filter, reflect.Map, reflect.Struct)
	if !isValidFilter {
		return nil, errors.New(
			"UpdateMany - Filter-argument must be a Map or Struct " +
				"(pointer or non-pointer)",
		)
	}
	isValidUpdate := verifyKind(update, reflect.Map)
	if !isValidUpdate {
		return nil, errors.New(
			"UpdateMany - Update-argument must be a Map (pointer or non-pointer)",
		)
	}

	encodedUpdate := &map[string]interface{}{
		"$set": update,
	}

	updateDoc, err := toBSON(encodedUpdate)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"UpdateMany - BSON Convert Error for update-argument",
		)
	}
	filterDoc, err := toBSON(filter)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"UpdateMany - BSON Convert Error for filter-argument",
		)
	}

	ctx, cancel := newTimeoutContext(c.Connection.Timeout)
	defer cancel()

	result, err := c.collection.UpdateMany(ctx, filterDoc, updateDoc)
	if err != nil {
		err = errors.Wrap(err, "UpdateMany Error")
	}
	return result, err
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
