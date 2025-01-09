package main

import (
	"context"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

//go:embed schema.gql
var schemaString string

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// initialize defradb
	defraDB, err := node.New(ctx, node.WithStoreType(node.MemoryStore))
	if err != nil {
		panic(err)
	}
	err = defraDB.Start(ctx)
	if err != nil {
		panic(err)
	}
	defer defraDB.Close(ctx)

	// add document log schema
	_, err = defraDB.DB.AddSchema(ctx, schemaString)
	if err != nil {
		panic(err)
	}
	// get the collection documents are stored in
	docCollection, err := defraDB.DB.GetCollectionByName(ctx, "MongoDocument")
	if err != nil {
		panic(err)
	}

	// connect to mongo instance in replica mode
	mongoClient, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017/?replicaSet=rs0"))
	if err != nil {
		panic(err)
	}

	sourceDB := mongoClient.Database("source")
	sinkDB := mongoClient.Database("sink")

	// listen for changes from mongo and inserts them into defra
	go func() {
		// listen for events and include the full document
		stream, err := sourceDB.Watch(ctx, mongo.Pipeline{}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
		if err != nil {
			panic(err)
		}
		defer stream.Close(ctx)
		// listen events until the stream errors or closes
		for stream.Next(ctx) {
			operationType := stream.Current.Lookup("operationType").StringValue()
			switch operationType {
			case "insert":
				doc, err := client.NewDocFromMap(map[string]any{
					"id":         stream.Current.Lookup("documentKey", "_id").ObjectID().Hex(),
					"collection": stream.Current.Lookup("ns", "coll").StringValue(),
					"document":   hex.EncodeToString(stream.Current.Lookup("fullDocument").Value),
				}, docCollection.Definition())
				if err != nil {
					panic(err)
				}
				err = docCollection.Create(ctx, doc)
				if err != nil {
					panic(err)
				}

			case "delete":
				collection := stream.Current.Lookup("ns", "coll").StringValue()
				id := stream.Current.Lookup("documentKey", "_id").ObjectID().Hex()

				filter := fmt.Sprintf(`{collection: {_eq: "%s"}, id: {_eq: "%s"}}`, collection, id)
				_, err := docCollection.DeleteWithFilter(ctx, filter)
				if err != nil {
					panic(err)
				}

			case "update", "replace":
				collection := stream.Current.Lookup("ns", "coll").StringValue()
				id := stream.Current.Lookup("documentKey", "_id").ObjectID().Hex()
				document := hex.EncodeToString(stream.Current.Lookup("fullDocument").Value)

				filter := fmt.Sprintf(`{collection: {_eq: "%s"}, id: {_eq: "%s"}}`, collection, id)
				patch := fmt.Sprintf(`{"document": "%s"}`, document)
				_, err = docCollection.UpdateWithFilter(ctx, filter, patch)
				if err != nil {
					panic(err)
				}
			}
		}
		if err := stream.Err(); err != nil {
			panic(err)
		}
	}()

	// listen for changes from defra and insert them into mongo
	go func() {
		sub, err := defraDB.DB.Events().Subscribe(event.MergeCompleteName, event.UpdateName)
		if err != nil {
			panic(err)
		}
		for evt := range sub.Message() {
			switch t := evt.Data.(type) {
			case event.MergeComplete:
				docID, err := client.NewDocIDFromString(t.Merge.DocID)
				if err != nil {
					panic(err)
				}
				doc, err := docCollection.Get(ctx, docID, true)
				if err != nil {
					panic(err)
				}
				idHex, err := doc.GetValue("id")
				if err != nil {
					panic(err)
				}
				fmt.Printf("\n\nevent.MergeComplete: %s\n\n", idHex.Value().(string))
				id, err := bson.ObjectIDFromHex(idHex.Value().(string))
				if err != nil {
					panic(err)
				}
				collection, err := doc.GetValue("collection")
				if err != nil {
					panic(err)
				}
				documentHex, err := doc.GetValue("document")
				if err != nil {
					panic(err)
				}
				document, err := hex.DecodeString(documentHex.Value().(string))
				if err != nil {
					panic(err)
				}
				sinkCollection := sinkDB.Collection(collection.Value().(string))
				_, err = sinkCollection.ReplaceOne(ctx, bson.M{"_id": id}, document, options.Replace().SetUpsert(true))
				if err != nil {
					panic(err)
				}

			case event.Update:
				docID, err := client.NewDocIDFromString(t.DocID)
				if err != nil {
					panic(err)
				}
				doc, err := docCollection.Get(ctx, docID, true)
				if err != nil {
					panic(err)
				}
				idHex, err := doc.GetValue("id")
				if err != nil {
					panic(err)
				}
				fmt.Printf("\n\nevent.Update: %s\n\n", idHex.Value().(string))
				id, err := bson.ObjectIDFromHex(idHex.Value().(string))
				if err != nil {
					panic(err)
				}
				collection, err := doc.GetValue("collection")
				if err != nil {
					panic(err)
				}
				documentHex, err := doc.GetValue("document")
				if err != nil {
					panic(err)
				}
				document, err := hex.DecodeString(documentHex.Value().(string))
				if err != nil {
					panic(err)
				}
				sinkCollection := sinkDB.Collection(collection.Value().(string))
				_, err = sinkCollection.ReplaceOne(ctx, bson.M{"_id": id}, document, options.Replace().SetUpsert(true))
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
}

// func encodeDocument(data bson.RawValue) (string, error) {
// 	var doc map[string]any
// 	err := bson.Unmarshal(data.Value, &doc)
// 	if err != nil {
// 		return "", err
// 	}
// 	delete(doc, "_id")
// 	out, err := bson.Marshal(doc)
// 	if err != nil {
// 		return "", err
// 	}
// 	return hex.EncodeToString(out), nil
// }
