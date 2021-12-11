# DB Updates -> Domain Events

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
docker compose --profile=tools run watch-topic dbserver1.inventory.customers
```
