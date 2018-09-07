package mongo

import (
	ctx "context"
	"reflect"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
)

// newTimeoutContext creates a new WithTimeout context with specified timeout.
func newTimeoutContext(timeout uint32) (ctx.Context, ctx.CancelFunc) {
	return ctx.WithTimeout(
		ctx.Background(),
		time.Duration(timeout)*time.Millisecond,
	)
}

// toBSON tries to convert a given interface{} to bson-document.
// If the interface{} contains the zero-ObjectID:
//  ObjectID("000000000000000000000000")
// then the ObjectID is removed so the mongo can generate a non-zero one automatically.
// A non-zero ObjectID is not removed.
func toBSON(data interface{}) (*bson.Document, error) {
	doc, err := bson.NewDocumentEncoder().EncodeDocument(data)
	if err != nil {
		return nil, err
	}

	// If no object ID is specified, delete the existing so it gets
	// automatically generated.
	dataObjectIDField := doc.Lookup("_id")

	if dataObjectIDField != nil {
		dataObjectID := dataObjectIDField.ObjectID().String()
		zeroObjectID := "ObjectID(\"000000000000000000000000\")"
		if dataObjectID == zeroObjectID {
			doc.Delete("_id")
		}
	}
	return doc, nil
}

// copyInterface creates a copy of a member of type:
//  interface{}
func copyInterface(intf interface{}) interface{} {
	intfType := reflect.TypeOf(intf)
	if intfType.Kind() == reflect.Ptr {
		// De-reference if its pointer
		intfType = reflect.TypeOf(intf).Elem()
	}
	return reflect.New(intfType).Interface()
}
