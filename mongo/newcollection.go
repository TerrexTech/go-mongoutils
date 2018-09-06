package mongo

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pkg/errors"

	"github.com/TerrexTech/go-commonutils/utils"
	"github.com/mongodb/mongo-go-driver/bson"
	mgo "github.com/mongodb/mongo-go-driver/mongo"
)

// EnsureCollection creates new collection with the provided indexes.
// If the collection already exists, it will just return the existing collection.
func EnsureCollection(c *Collection) (*Collection, error) {
	if c == nil {
		return nil, errors.New("Collection argument cannot be nil")
	}
	err := verifySchemaStruct(c.SchemaStruct)
	if err != nil {
		return nil, errors.Wrap(err, "Schema Verification Error:")
	}
	err = verifyIndexKeys(c.SchemaStruct, c.Indexes)
	if err != nil {
		return nil, errors.Wrap(err, "Index-Keys Validation Error:")
	}

	c.collection = c.Connection.Client.
		Database(c.Database).
		Collection(c.Name)

	ctx, cancel := newTimeoutContext(c.Connection.Timeout)
	defer cancel()

	if c.Indexes != nil {
		for _, indexConfig := range c.Indexes {
			indexOptions := bson.NewDocument(
				bson.EC.Boolean("unique", indexConfig.IsUnique),
			)
			if indexConfig.Name != "" {
				indexOptions.Append(bson.EC.String("name", indexConfig.Name))
			}

			indexes := c.collection.Indexes()
			err := createIndex(
				ctx,
				&indexConfig.ColumnConfig,
				&indexes,
				indexOptions,
			)
			if err != nil {
				cancel()
				return nil, err
			}
		}
	}
	return c, nil
}

func verifySchemaStruct(schemaStruct interface{}) error {
	if schemaStruct == nil {
		return errors.New("SchemaStruct cannot be nil")
	}

	isSchemaPtr := reflect.TypeOf(schemaStruct).Kind() == reflect.Ptr
	if !isSchemaPtr {
		return errors.New("SchemaStruct needs to be a pointer to struct")
	}
	// De-reference pointer and check if its struct
	isSchemaStruct := reflect.ValueOf(schemaStruct).
		Elem().
		Type().
		Kind() == reflect.Struct
	if !isSchemaStruct {
		return errors.New("SchemaStruct needs to be a pointer to struct")
	}

	return nil
}

// verifyIndexKeys ensures that the keys specified in an index are also present in SchemaStruct.
func verifyIndexKeys(schemaStruct interface{}, indexConfigs []IndexConfig) error {
	structDoc, err := bson.NewDocumentEncoder().
		EncodeDocument(schemaStruct)
	if err != nil {
		return err
	}
	collectionKeys, err := structDoc.Keys(false)
	if err != nil {
		return err
	}

	strKeys := []string{}
	for _, key := range collectionKeys {
		strKeys = append(strKeys, key.Name)
	}

	for _, indexConfig := range indexConfigs {
		for _, colConfig := range indexConfig.ColumnConfig {
			isValid := utils.IsElementInSlice(strKeys, colConfig.Name)

			if !isValid {
				return fmt.Errorf(
					"Error: IndexKey: %s not found in specified collection-keys",
					colConfig.Name,
				)
			}
		}
	}

	return nil
}

func createIndex(
	ctx context.Context,
	indexColumns *[]IndexColumnConfig,
	indexes *mgo.IndexView,
	indexOptions *bson.Document,
) error {
	indexBson := bson.NewDocument()
	for _, column := range *indexColumns {
		var sortOrder int32 = 1
		if column.IsDescOrder {
			sortOrder = -1
		}

		indexBson.Append(
			bson.EC.Int32(column.Name, sortOrder),
		)
	}
	// We are not using #CreateMany to be able to apply configs
	// on individual-index basis.
	_, err := indexes.CreateOne(
		ctx,
		mgo.IndexModel{
			Keys:    indexBson,
			Options: indexOptions,
		},
	)

	if err != nil {
		return err
	}
	return nil
}
