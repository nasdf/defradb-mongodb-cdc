# DefraDB MongoDB change data capture

This repo contains a proof of concept for change data capture from MongoDB using DefraDB as the change log and synchronization engine.

MongoDB is configured as a single node replica so that DefraDB can listen for events from the change stream. The `docker-compose.yml` file contains an example of how to configure the MongoDB replica node.

