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

## Configuration file

```yaml
meilisearch:
    url: "http://localhost:7700"
    user: ""
    password: ""
```
