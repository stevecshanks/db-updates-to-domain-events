# DB Updates -> Domain Events

A simple example app built on top of the [Debezium tutorial](https://github.com/debezium/debezium-examples/tree/main/tutorial). Updates to the database will be captured, and domain events will be published to a Kafka topic.

Use the tooling below to make changes to the `products_on_hand` table. Interesting events (like a product going out of stock) will be published to the `stock-notifications` topic.

## Setup

To run:

```bash
docker compose up -d
```

Then, in another terminal:

```bash
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json
```

## Tooling

To access the database:

```bash
docker compose --profile=tools run psql
```

To watch a topic:

```bash
docker compose --profile=tools run watch-topic <topic-name>
```

e.g.

```bash
docker compose --profile=tools run watch-topic stock-notifications
```
