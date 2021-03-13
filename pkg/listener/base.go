package listener

import cb "github.com/hyperledger/fabric-protos-go/common"

type Item struct {
	ID          string      `json:"id"`
	Data        interface{} `json:"data"`
	TXID        string      `json:"txid"`
	BlockNumber int         `json:"blockNumber"`
	TXDate      int         `json:"tx_date"`
}
type ItemHistory struct {
	Data    interface{}
	TXID    string `json:"txid"`
	BlockID string `json:"blockId"`
	TXDate  string `json:"tx_date"`
}

type BlockStorage interface {
	Store(block *cb.Block) error
	StoreBulk(blocks []*cb.Block) error
}
