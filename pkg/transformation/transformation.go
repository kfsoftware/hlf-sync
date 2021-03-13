package transformation

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/kfsoftware/hlf-sync/internal/github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/kfsoftware/hlf-sync/internal/github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

type Document struct {
	TXDate      int
	TXID        string
	BlockNumber int
	ChannelID   string
	Data        map[string]interface{}
	ChaincodeID string
	PrimaryKey  string
}
type DocumentExtractionResponse struct {
	DocumentsToAdd    map[string]*Document
	DocumentsToRemove map[string]*Document
}

const (
	PrimaryKey = "_fabric_id"
	DateKey    = "_fabric_date"
	TxIDKey    = "_fabric_txid"
)

func merge(ms ...map[string]*Document) map[string]*Document {
	res := map[string]*Document{}
	for _, m := range ms {
		for k, v := range m {
			res[k] = v
		}
	}
	return res
}
func BlocksToDocuments(blocks []*cb.Block) (*DocumentExtractionResponse, error) {
	response := &DocumentExtractionResponse{
		DocumentsToAdd:    map[string]*Document{},
		DocumentsToRemove: map[string]*Document{},
	}

	for _, block := range blocks {
		r, err := BlockToDocuments(block)
		if err != nil {
			return nil, err
		}
		response.DocumentsToAdd = merge(response.DocumentsToAdd, r.DocumentsToAdd)
		response.DocumentsToRemove = merge(response.DocumentsToRemove, r.DocumentsToRemove)
	}

	return response, nil
}

func BlockToDocuments(block *cb.Block) (*DocumentExtractionResponse, error) {
	response := &DocumentExtractionResponse{
		DocumentsToAdd:    map[string]*Document{},
		DocumentsToRemove: map[string]*Document{},
	}

	for _, txData := range block.Data.Data {
		env := &cb.Envelope{}
		err := proto.Unmarshal(txData, env)
		if err != nil {
			return nil, err
		}

		payload := &cb.Payload{}
		err = proto.Unmarshal(env.Payload, payload)
		if err != nil {
			return nil, err
		}
		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return nil, err
		}
		channelHeader := &cb.ChannelHeader{}
		if err := proto.Unmarshal(payload.Header.ChannelHeader, channelHeader); err != nil {
			return nil, errors.Wrap(err, "unmarshal payload from envelope failed")
		}
		txID := channelHeader.TxId
		txDate, err := ptypes.Timestamp(chdr.Timestamp)
		if err != nil {
			return nil, err
		}
		txDateMS := txDate.UnixNano() / int64(time.Millisecond)
		switch cb.HeaderType(chdr.Type) {
		case cb.HeaderType_MESSAGE:
			log.Debugf("HeaderType_MESSAGE ignored")
		case cb.HeaderType_CONFIG:
			log.Debugf("HeaderType_CONFIG ignored")
		case cb.HeaderType_CONFIG_UPDATE:
			log.Debugf("HeaderType_CONFIG_UPDATE ignored")
		case cb.HeaderType_ENDORSER_TRANSACTION:
			action, err := protoutil.GetActionFromEnvelopeMsg(env)
			if err != nil {
				log.Debugf("Failed to get action %v", err)
			} else {
				txRWSet := &rwsetutil.TxRwSet{}
				err = txRWSet.FromProtoBytes(action.Results)
				if err != nil {
					return nil, err
				}
				for _, set := range txRWSet.NsRwSets {
					chaincodeID := set.NameSpace

					for _, write := range set.KvRwSet.Writes {
						var data map[string]interface{}
						err = json.Unmarshal(write.Value, &data)
						if err != nil {
							dataStr := string(write.Value)
							data = map[string]interface{}{
								"value": dataStr,
							}
						}
						data[TxIDKey] = txID
						data[DateKey] = txDateMS
						compositeKey := "\u0000"
						key := strings.Trim(write.Key, compositeKey)
						key = strings.Replace(key, compositeKey, "__", -1)
						data[PrimaryKey] = key
						document := &Document{
							ChannelID:   chdr.ChannelId,
							Data:        data,
							ChaincodeID: chaincodeID,
							PrimaryKey:  key,
							TXID:        txID,
							TXDate:      int(txDateMS),
							BlockNumber: int(block.Header.Number),
						}
						if write.IsDelete {
							response.DocumentsToRemove[key] = document
							delete(response.DocumentsToAdd, key)
						} else {
							response.DocumentsToAdd[key] = document
							delete(response.DocumentsToRemove, key)
						}
					}
				}
			}
		case cb.HeaderType_ORDERER_TRANSACTION:
			log.Debugf("HeaderType_ORDERER_TRANSACTION ignored")
		case cb.HeaderType_DELIVER_SEEK_INFO:
			log.Debugf("HeaderType_DELIVER_SEEK_INFO ignored")
		case cb.HeaderType_CHAINCODE_PACKAGE:
			log.Debugf("HeaderType_CHAINCODE_PACKAGE ignored")
		}

	}
	return response, nil
}
