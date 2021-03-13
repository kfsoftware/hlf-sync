package listener

import (
	"github.com/gogo/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/kfsoftware/hlf-sync/pkg/mocks"
	"github.com/meilisearch/meilisearch-go"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFoo(t *testing.T) {
	var meiliClient = meilisearch.NewClient(meilisearch.Config{
		Host:   "http://127.0.0.1:7700",
		APIKey: "meilisearch123",
	})
	meiliStorage := MeilisearchStorage{
		client: meiliClient,
	}
	channelID := "mychannel"
	envBytes, err := proto.Marshal(
		mocks.NewTx(
			channelID,
			&mocks.TXInfo{
				TxID:             "122",
				TxValidationCode: pb.TxValidationCode_VALID,
				HeaderType:       cb.HeaderType_ENDORSER_TRANSACTION,
				ChaincodeID:      "test_fabcar",
			},
		),
	)
	if err != nil {
		panic(err)
	}
	data := [][]byte{
		envBytes,
	}
	txValidationFlags := []uint8{
		uint8(pb.TxValidationCode_VALID),
	}
	blockMetaData := make([][]byte, 4)
	blockMetaData[cb.BlockMetadataIndex_TRANSACTIONS_FILTER] = txValidationFlags

	blk := &cb.Block{
		Header:   &cb.BlockHeader{},
		Metadata: &cb.BlockMetadata{Metadata: blockMetaData},
		Data:     &cb.BlockData{Data: data},
	}

	err = meiliStorage.Store(blk)
	assert.NoError(t, err)
}
