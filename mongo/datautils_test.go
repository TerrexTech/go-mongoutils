package mongo

import (
	"context"
	"time"

	"github.com/mongodb/mongo-go-driver/bson/objectid"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MongoUtils", func() {
	Describe("newTimeoutContext", func() {
		It("should return WithTimeout context with specified timeout", func() {
			Context("deadline exceeds", func() {
				timeout := uint32(20) // Milliseconds
				ctx, cancel := newTimeoutContext(timeout)
				defer cancel()

				errChan := make(chan error)
				go func(timeoutCtx context.Context) {
					for {
						select {
						case <-timeoutCtx.Done():
							errChan <- timeoutCtx.Err()
							break
						default:
							time.Sleep(30 * time.Millisecond)
							break
						}
					}
				}(ctx)
				Expect(<-errChan).To(HaveOccurred())
			})

			Context("deadline not does exceed", func() {
				timeout := uint32(20) // Milliseconds
				ctx, cancel := newTimeoutContext(timeout)
				defer cancel()

				strChan := make(chan string)
				go func(timeoutCtx context.Context) {
					for {
						select {
						case <-timeoutCtx.Done():
							break
						default:
							time.Sleep(3 * time.Millisecond)
							strChan <- "response"
							break
						}
					}
				}(ctx)
				Expect(<-strChan).To(Equal("response"))
			})
		})
	})

	Describe("toBSON", func() {
		It("should exclude _id field if its not set", func() {
			type test struct {
				ID  objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
				Num int32             `bson:"num" json:"num"`
				Str string            `bson:"str" json:"str"`
			}

			t := &test{
				Num: 1,
				Str: "test",
			}
			doc, err := toBSON(t)
			Expect(err).ToNot(HaveOccurred())
			Expect(doc.Lookup("_id")).To(BeNil())
			Expect(doc.Lookup("str").StringValue()).To(Equal(t.Str))
			Expect(doc.Lookup("num").Int32()).To(Equal(t.Num))
		})

		It("should exclude _id field if its set", func() {
			type test struct {
				ID  objectid.ObjectID `bson:"_id,omitempty" json:"_id,omitempty"`
				Num int32             `bson:"num" json:"num"`
				Str string            `bson:"str" json:"str"`
			}

			t := &test{
				ID:  objectid.New(),
				Num: 1,
				Str: "test",
			}
			doc, err := toBSON(t)
			Expect(err).ToNot(HaveOccurred())
			Expect(doc.Lookup("_id").ObjectID()).To(Equal(t.ID))
			Expect(doc.Lookup("str").StringValue()).To(Equal(t.Str))
			Expect(doc.Lookup("num").Int32()).To(Equal(t.Num))
		})
	})

	Describe("copyInterface", func() {
		Context("pointer interface is provided", func() {
			It("should create copy of provided pointer interface", func() {
				type testStruct struct {
					a string
				}

				ts := &testStruct{
					a: "original",
				}
				copyts := copyInterface(ts).(*testStruct)
				copyts.a = "changed"

				Expect(ts.a).ToNot(Equal(copyts.a))
			})

			It("should create copy of provided non-pointer interface", func() {
				type testStruct struct {
					a string
				}

				ts := testStruct{
					a: "original",
				}
				copyts := copyInterface(ts).(*testStruct)
				copyts.a = "changed"

				Expect(ts.a).ToNot(Equal(copyts.a))
			})
		})
	})
})
