package transformation

import (
	"encoding/json"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/kfsoftware/hlf-sync/pkg/mocks"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMapping(t *testing.T) {
	channelID := "mychannel"
	chID := "fabcar"
	k2Data := map[string]interface{}{
		"subtitle": "foobar",
		"title":    "Foobar",
		"items":    []interface{}{"1", "2", "3", "4"},
	}
	k2Bytes, err := json.Marshal(k2Data)
	assert.NoError(t, err)
	keyInsert := "K1"
	keyDelete := "K2"
	results := mocks.GetTxResults(
		chID,
		[]*kvrwset.KVWrite{
			{
				Key:      keyInsert,
				IsDelete: false,
				Value:    k2Bytes,
			},
			{
				Key:      keyDelete,
				IsDelete: true,
			},
		},
	)
	txID := "12"
	txInfo := &mocks.TXInfo{
		TxID:             txID,
		TxValidationCode: pb.TxValidationCode_VALID,
		HeaderType:       cb.HeaderType_ENDORSER_TRANSACTION,
		ChaincodeID:      chID,
		Results:          results,
	}
	blk := mocks.NewBlock(
		channelID,
		txInfo,
	)
	response, err := BlockToDocuments(blk)
	assert.NoError(t, err)
	assert.Len(t, response.DocumentsToAdd, 1)
	assert.Len(t, response.DocumentsToRemove, 1)
	assert.Equal(t, response.DocumentsToAdd[keyInsert].PrimaryKey, "K1")
	assert.Equal(t, response.DocumentsToAdd[keyInsert].TXID, txID)
	assert.Equal(t, response.DocumentsToAdd[keyInsert].ChannelID, channelID)
	data := response.DocumentsToAdd[keyInsert].Data
	delete(data, PrimaryKey)
	delete(data, TxIDKey)
	delete(data, DateKey)
	assert.Equal(t, data, k2Data)
	assert.Equal(t, response.DocumentsToRemove[keyDelete].PrimaryKey, "K2")
	assert.Equal(t, response.DocumentsToRemove[keyDelete].TXID, txID)
	assert.Equal(t, response.DocumentsToRemove[keyDelete].ChannelID, channelID)
}
