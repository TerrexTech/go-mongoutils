package mongo

import (
	ctx "context"
	"reflect"
	"time"
)

// newTimeoutContext creates a new WithTimeout context with specified timeout.
func newTimeoutContext(timeout uint32) (ctx.Context, ctx.CancelFunc) {
	return ctx.WithTimeout(
		ctx.Background(),
		time.Duration(timeout)*time.Millisecond,
	)
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

// verifyKind returns true if the provided interface{} matches the
// provided type(s).
func verifyKind(intf interface{}, validKinds ...reflect.Kind) bool {
	kind := reflect.TypeOf(intf).Kind()
	// Deref if its a pointer
	if kind == reflect.Ptr {
		kind = reflect.TypeOf(intf).Elem().Kind()
	}

	isMatched := false
	for _, k := range validKinds {
		if kind == k {
			isMatched = true
			break
		}
	}

	return isMatched
}
