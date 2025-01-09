# DefraDB MongoDB change data capture

This repo contains a proof of concept for change data capture from MongoDB. It uses DefraDB as an eventually consistent operation log and synchronization engine.

MongoDB is configured as a single node replica so we can listen for events from the change stream. The `docker-compose.yml` file contains an example of how to configure the MongoDB replica node.

The implementation uses two go routines to represent processes that could be run on different machines over a network. They are both implemented in a single file to make it easier to understand.

## MongoDB source process

On every event (insert, update, delete, replace) a document is created or updated in the DefraDB instance. The document contains the name of the MongoDB collection, unique `_id` of the document, and the BSON encoded MongoDB document. See the [schema.gql](./schema.gql) file for a full schema description.

## DefraDB sink process

On every event (update, merge complete) a document is inserted, deleted, or replaced in the MongoDB instance. The document is reconstructed using the latest version of the document created or updated in the first process. The documents in the MongoDB instance inherit the eventual consistency of the CRDTs found in DefraDB, but at the transaction level instead of field level.

## Running

Start MongoDB:

```bash
$ docker-compose up
```

Run the main file:

```bash
$ go run main.go
```
