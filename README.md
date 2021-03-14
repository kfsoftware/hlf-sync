# HLF Exporter

## Introduction

Hyperledger Fabric stores the information in blocks, but this information is not structured and lacks search/processing capabilities of new databases.

This project aims to store all the information in an offchain database to access the blockchain data aswell as add as a means to see the data for other purposes, such as validating, dashboards, statistics, etcetera. 

## Get started

Pre requisites:
- A running Hyperledger Fabric network 
- A running supported database

```bash
hlf-sync --network=./hlf.yaml --config=config.yaml
``` 

## Databases supported

- [x] Elasticsearch
- [x] PostgreSQL
- [x] MySQL
- [x] MariaDB
- [x] Meilisearch

## Network Config

Network config file needs to be compliant with fabric-sdk-go. You can find examples in [the official repo](https://github.com/hyperledger/fabric-sdk-go/blob/main/test/fixtures/config/config_e2e.yaml).

## Configuration file
The configuration file for a meilisearch backend
```yaml
database:
  type: meilisearch
  url: "http://localhost:7700"
  apiKey: ""
```

The configuration file for a postgresql backend
```yaml
database:
  type: sql
  driver: postgres
  dataSource: host=localhost port=5432 user=postgres password=postgres dbname=hlf sslmode=disable

```
The configuration file for a mysql backend
```yaml
database:
  type: sql
  driver: mysql
  dataSource: root:my-secret-pw@tcp(127.0.0.1:3306)/hlf?charset=utf8mb4&parseTime=True&loc=Local
```

The configuration file for a mariadb backend
```yaml
database:
  type: sql
  driver: mysql
  dataSource: root:my-secret-pw@tcp(127.0.0.1:3306)/hlf?charset=utf8mb4&parseTime=True&loc=Local
```

The configuration file for an Elasticsearch backend
```yaml
database:
  type: elasticsearch
  urls:
    - http://localhost:9200
  user:
  password:

```

