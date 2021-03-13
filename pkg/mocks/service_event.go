package mocks

import (
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric-protos-go/peer"
	"github.com/kfsoftware/hlf-sync/internal/github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"time"
)

func NewTxAction(ccID string, results []byte) *pb.TransactionAction {

	chaincodeAction := &pb.ChaincodeAction{
		ChaincodeId: &pb.ChaincodeID{
			Name: ccID,
		},
		Results: results,
	}
	extBytes, err := proto.Marshal(chaincodeAction)
	if err != nil {
		panic(err)
	}
	prp := &pb.ProposalResponsePayload{
		Extension: extBytes,
	}
	prpBytes, err := proto.Marshal(prp)
	if err != nil {
		panic(err)
	}
	chActionPayload := &pb.ChaincodeActionPayload{
		Action: &pb.ChaincodeEndorsedAction{
			ProposalResponsePayload: prpBytes,
		},
	}
	payloadBytes, err := proto.Marshal(chActionPayload)
	if err != nil {
		panic(err)
	}

	return &pb.TransactionAction{
		Payload: payloadBytes,
		Header:  nil,
	}
}

type TXInfo struct {
	TxID             string
	ChaincodeID      string
	TxValidationCode pb.TxValidationCode
	HeaderType       cb.HeaderType
	Results          []byte
}

func NewTx(
	channelID string,
	txInfo *TXInfo,
) *cb.Envelope {
	tx := &pb.Transaction{
		Actions: []*pb.TransactionAction{NewTxAction(txInfo.ChaincodeID, txInfo.Results)},
	}
	txBytes, err := proto.Marshal(tx)
	if err != nil {
		panic(err)
	}
	timestamp, err := ptypes.TimestampProto(time.Now().UTC())
	if err != nil {
		panic(err)
	}
	channelHeader := &cb.ChannelHeader{
		ChannelId: channelID,
		TxId:      txInfo.TxID,
		Type:      int32(txInfo.HeaderType),
		Timestamp: timestamp,
	}
	channelHeaderBytes, err := proto.Marshal(channelHeader)
	if err != nil {
		panic(err)
	}

	payload := &cb.Payload{
		Header: &cb.Header{
			ChannelHeader: channelHeaderBytes,
		},
		Data: txBytes,
	}
	payloadBytes, err := proto.Marshal(payload)
	if err != nil {
		panic(err)
	}

	return &cb.Envelope{
		Payload: payloadBytes,
	}
}

func GetTxResults(chaincodeID string, writes []*kvrwset.KVWrite) []byte {
	txRWSet := &rwsetutil.TxRwSet{}
	txRWSet.NsRwSets = append(
		txRWSet.NsRwSets,
		&rwsetutil.NsRwSet{
			NameSpace: chaincodeID,
			KvRwSet: &kvrwset.KVRWSet{
				Reads: []*kvrwset.KVRead{
					{
						Key: "1",
						Version: &kvrwset.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
				RangeQueriesInfo: nil,
				Writes:           writes,
				MetadataWrites:   []*kvrwset.KVMetadataWrite{},
			},
			CollHashedRwSets: nil,
		},
	)
	txRWBytes, err := txRWSet.ToProtoBytes()
	if err != nil {
		panic(err)
	}
	return txRWBytes
}
func NewBlock(channelID string, transactions ...*TXInfo) *cb.Block {
	var data [][]byte
	txValidationFlags := make([]uint8, len(transactions))
	for i, txInfo := range transactions {
		envBytes, err := proto.Marshal(NewTx(channelID, txInfo))
		if err != nil {
			panic(err)
		}
		data = append(data, envBytes)
		txValidationFlags[i] = uint8(txInfo.TxValidationCode)
	}

	blockMetaData := make([][]byte, 4)
	blockMetaData[cb.BlockMetadataIndex_TRANSACTIONS_FILTER] = txValidationFlags

	return &cb.Block{
		Header:   &cb.BlockHeader{},
		Metadata: &cb.BlockMetadata{Metadata: blockMetaData},
		Data:     &cb.BlockData{Data: data},
	}
}
