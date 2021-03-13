package listener

import (
	"bytes"
	"encoding/json"
	"fmt"
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/kfsoftware/hlf-sync/pkg/transformation"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type ElasticSearchStorage struct {
	client *elasticsearch7.Client
}

func (e ElasticSearchStorage) StoreBulk(blocks []*cb.Block) error {
	docs, err := transformation.BlocksToDocuments(blocks)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, document := range docs.DocumentsToAdd {
		key := IndexKey{
			ChaincodeID: document.ChaincodeID,
			ChannelID:   document.ChannelID,
		}
		indexName := fmt.Sprintf("%s_%s", key.ChannelID, key.ChaincodeID)

		data, err := json.Marshal(document.Data)
		if err != nil {
			return err
		}
		meta := []byte(fmt.Sprintf(`{ "index" : {"_index": "%s",  "_id" : "%s" } }%s`, indexName, document.PrimaryKey, "\n"))
		data = append(data, "\n"...)
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	for _, document := range docs.DocumentsToRemove {
		key := IndexKey{
			ChaincodeID: document.ChaincodeID,
			ChannelID:   document.ChannelID,
		}
		indexName := fmt.Sprintf("%s_%s", key.ChannelID, key.ChaincodeID)

		var buf bytes.Buffer
		buf.Write(
			[]byte(
				fmt.Sprintf(`{ "delete" : { "_index" : "%s", "_id" : "%s" } }%s`, indexName, document.PrimaryKey, "\n"),
			),
		)
	}
	log.Infof("Items added=%d", len(docs.DocumentsToAdd))
	log.Infof("Items removed=%d", len(docs.DocumentsToRemove))
	if buf.Len() > 0 {
		res, err := e.client.Bulk(bytes.NewReader(buf.Bytes()))
		if err != nil {
			return err
		}
		if res.IsError() {
			var raw map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
				return errors.Errorf("Failure to  parse response body: %s", err)
			} else {
				return errors.Errorf("  Error: [%d] %s: %s",
					res.StatusCode,
					raw["error"].(map[string]interface{})["type"],
					raw["error"].(map[string]interface{})["reason"],
				)
			}
		}
	}
	return nil
}

func NewElasticStorage(client *elasticsearch7.Client) ElasticSearchStorage {
	return ElasticSearchStorage{
		client: client,
	}
}
func (e ElasticSearchStorage) Store(block *cb.Block) error {
	docs, err := transformation.BlockToDocuments(block)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, document := range docs.DocumentsToAdd {
		key := IndexKey{
			ChaincodeID: document.ChaincodeID,
			ChannelID:   document.ChannelID,
		}
		indexName := fmt.Sprintf("%s_%s", key.ChannelID, key.ChaincodeID)

		data, err := json.Marshal(document.Data)
		if err != nil {
			return err
		}
		meta := []byte(fmt.Sprintf(`{ "index" : {"_index": "%s",  "_id" : "%s" } }%s`, indexName, document.PrimaryKey, "\n"))
		data = append(data, "\n"...)
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	for _, document := range docs.DocumentsToRemove {
		key := IndexKey{
			ChaincodeID: document.ChaincodeID,
			ChannelID:   document.ChannelID,
		}
		indexName := fmt.Sprintf("%s_%s", key.ChannelID, key.ChaincodeID)

		var buf bytes.Buffer
		buf.Write(
			[]byte(
				fmt.Sprintf(`{ "delete" : { "_index" : "%s", "_id" : "%s" } }%s`, indexName, document.PrimaryKey, "\n"),
			),
		)
	}
	if buf.Len() > 0 {
		res, err := e.client.Bulk(bytes.NewReader(buf.Bytes()))
		if err != nil {
			return err
		}
		if res.IsError() {
			var raw map[string]interface{}
			if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
				return errors.Errorf("Failure to  parse response body: %s", err)
			} else {
				return errors.Errorf("  Error: [%d] %s: %s",
					res.StatusCode,
					raw["error"].(map[string]interface{})["type"],
					raw["error"].(map[string]interface{})["reason"],
				)
			}
		}
	}

	return nil
}
