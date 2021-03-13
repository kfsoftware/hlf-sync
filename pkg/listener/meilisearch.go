package listener

import (
	"context"
	"fmt"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/kfsoftware/hlf-sync/pkg/transformation"
	"github.com/meilisearch/meilisearch-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"math"
	"time"
)

type MeilisearchStorage struct {
	client    meilisearch.ClientInterface
	indexName string
}

func NewMeilisearchStorage(client meilisearch.ClientInterface, channelID string) (MeilisearchStorage, error) {
	indexName := fmt.Sprintf("%s", channelID)
	storage := MeilisearchStorage{
		client:    client,
		indexName: indexName,
	}
	_, err := storage.createIndex(indexName)
	if err != nil {
		return storage, err
	}
	return storage, nil
}

func (m MeilisearchStorage) createIndex(indexName string) (*meilisearch.Index, error) {
	index, err := m.client.Indexes().Get(indexName)
	if err != nil {
		meilieErr := errors.Cause(err).(*meilisearch.Error)
		log.Print(meilieErr)
		if meilieErr.StatusCode == 404 {
			// not found
		} else {
			return nil, err
		}
	} else {
		return index, nil
	}
	responseIndex, err := m.client.Indexes().Create(meilisearch.CreateIndexRequest{
		UID:        indexName,
		PrimaryKey: transformation.PrimaryKey,
		Name:       indexName,
	})
	if err != nil {
		return nil, err
	}
	err = m.waitForUpdate(responseIndex.UpdateID)
	if err != nil {
		return nil, err
	}
	asyncUpdate, err := m.client.Settings(index.Name).UpdateRankingRules([]string{"desc(_fabric_date)"})
	if err != nil {
		return nil, err
	}
	err = m.waitForUpdate(asyncUpdate.UpdateID)
	if err != nil {
		return nil, err
	}
	index, err = m.client.Indexes().Get(responseIndex.UID)
	if err != nil {
		return nil, err
	}
	return index, nil
}

type IndexKey struct {
	ChaincodeID string
	ChannelID   string
}
type IndexDoc = map[string]interface{}

func (m MeilisearchStorage) storeDocs(response *transformation.DocumentExtractionResponse) error {
	var documentsToAdd []IndexDoc
	var documentsToRemove []string
	keyDocsAdded := []string{}
	for _, document := range response.DocumentsToAdd {
		if document.ChaincodeID == "lscc" || document.ChaincodeID == "_lifecycle" {
			continue
		}
		documentsToAdd = append(
			documentsToAdd,
			document.Data,
		)
		keyDocsAdded = append(keyDocsAdded, document.PrimaryKey)
	}
	for _, document := range response.DocumentsToRemove {
		if document.ChaincodeID == "lscc" || document.ChaincodeID == "_lifecycle" {
			continue
		}
		documentsToRemove = append(
			documentsToRemove,
			document.PrimaryKey,
		)
	}

	if len(documentsToAdd) > 0 {
		updateRes, err := m.client.Documents(m.indexName).AddOrUpdate(documentsToAdd)
		if err != nil {
			return err
		}
		err = m.waitForUpdate(updateRes.UpdateID)
		if err != nil {
			return err
		}
	}
	if len(documentsToRemove) > 0 {
		updateRes, err := m.client.Documents(m.indexName).Deletes(documentsToRemove)
		if err != nil {
			return err
		}
		err = m.waitForUpdate(updateRes.UpdateID)
		if err != nil {
			return err
		}
	}

	log.Infof("Items added=%d %v", len(response.DocumentsToAdd), keyDocsAdded[:int(math.Min(float64(10), float64(len(keyDocsAdded))))])
	log.Infof("Items removed=%d", len(response.DocumentsToRemove))
	return nil
}

func (m MeilisearchStorage) waitForUpdate(updateID int64) error {
	ctx := context.Background()
	log.Debugf("Update ID: %d", updateID)
	updateStatus, err := m.client.WaitForPendingUpdate(
		ctx,
		200*time.Millisecond,
		m.indexName,
		&meilisearch.AsyncUpdateID{UpdateID: int64(updateID)},
	)
	if err != nil {
		return err
	}
	log.Debugf("Update %d=%s", updateID, updateStatus)
	return nil
}

func (m MeilisearchStorage) StoreBulk(blocks []*cb.Block) error {
	response, err := transformation.BlocksToDocuments(blocks)
	if err != nil {
		return err
	}
	err = m.storeDocs(response)
	if err != nil {
		return err
	}
	return nil
}
func (m MeilisearchStorage) Store(block *cb.Block) error {
	response, err := transformation.BlockToDocuments(block)
	if err != nil {
		return err
	}
	err = m.storeDocs(response)
	if err != nil {
		return err
	}
	return nil
}
