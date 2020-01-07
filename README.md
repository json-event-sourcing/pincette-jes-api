# HTTP Handling For JSON Event Sourcing

The [JSON Event Sourcing](https://github.com/json-event-sourcing/pincette-jes) solution works with [Kafka](https://kafka.apache.org) and [MongoDB](https://www.mongodb.com). The library in this package provides the HTTP handling for that environment. You can use it to easily enable that logic in a Java HTTP server solution. An example is the [pincette-jes-http](https://github.com/json-event-sourcing/pincette-jes-http) repository, which uses a [Netty HTTP server](https://github.com/json-event-sourcing/pincette-netty-http) and delegates everything to this library.

The URL path for an aggregate always has the form ```[/context]/application/aggregate_type[/id]```. When the identifier is set, one specific aggregate instance is addressed. Without it the complete collection of aggregates of that type is addressed. For example, a GET would return an array of all the aggregate instances.

When aggregates change because of a command the result is sent back through the Kafka reply topic. It is possible to push this to the client using [Server-Sent Events](https://www.w3.org/TR/eventsource/). This is done with the [fanout.io](https://fanout.io) service. The endpoint ```/sse``` is where the client should connect to. It will be redirected to the fanout.io service with the username and the JWT in a URL parameter. Then fanout.io comes back to the ```/sse-setup``` endpoint, where the channel is created.

Requests need to have a valid [JSON Web Token](https://jwt.io). They are taken from the bearer token in the ```Authorization``` header. Then the URL parameter ```access_token``` is tried and finally the cookie with the same name is considered.

If the header ```X-Pincette-JES-OnBehalfOf``` is present and if the calling user is "system", then the value of the header will be used to add the ```_jwt``` field to the command.

## Methods

|Path|Method|Description|
|---|---|---|
|/app/type/id|GET|Fetches an aggregate instance.|
| |POST|Sends a command to an aggregate instance.|
| |PUT|Replaces an aggregate instance.|
|/app/type|GET|Fetches all aggregate instances of a type, as a JSON array.|
| |POST|Performs a search in a collection of aggregates of some type. The request body should be a JSON array containing [MongoDB aggregation pipeline stages](https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/).|
|/sse|GET|The Server-Sent Events endpoint for clients.|
|/sse-setup|GET|The Server-Sent Events endpoint for fanout.io.|
