package listener

import (
	"encoding/json"
	"fmt"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/kfsoftware/hlf-sync/pkg/transformation"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gorm.io/datatypes"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"math"
	"time"
)

type DatabaseStorage struct {
	tableName string
	db        *gorm.DB
}
type DriverName string

const (
	PostgresqlDriver = "postgres"
	MySQLDriver      = "mysql"
)

type Record struct {
	ID        string
	Data      datatypes.JSON
	Chaincode string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewPostgresStorage(driverName DriverName, dataSourceName string, channelID string) (DatabaseStorage, error) {
	var db *gorm.DB
	var err error
	newLogger := logger.New(
		log.New(),
		logger.Config{
			SlowThreshold: time.Second,
			LogLevel:      logger.Silent,
			Colorful:      false,
		},
	)
	gormConfig := &gorm.Config{
		Logger: newLogger,
	}
	switch driverName {
	case PostgresqlDriver:
		db, err = gorm.Open(
			postgres.New(
				postgres.Config{
					DSN:                  dataSourceName,
					PreferSimpleProtocol: true,
				},
			),
			gormConfig,
		)
		if err != nil {
			return DatabaseStorage{}, err
		}
	case MySQLDriver:
		db, err = gorm.Open(mysql.Open(dataSourceName), gormConfig)
		if err != nil {
			return DatabaseStorage{}, err
		}
	default:
		return DatabaseStorage{}, errors.Errorf("Driver %s not supported", string(driverName))

	}
	tableName := fmt.Sprintf("%s", channelID)
	storage := DatabaseStorage{
		db:        db,
		tableName: tableName,
	}
	err = db.Table(tableName).AutoMigrate(&Record{})
	if err != nil {
		return storage, err
	}
	return storage, nil
}

func (m DatabaseStorage) storeDocs(response *transformation.DocumentExtractionResponse) error {
	var recordsToAdd []Record
	var recordsToRemove []string
	var keyDocsAdded []string
	for _, document := range response.DocumentsToAdd {
		if document.ChaincodeID == "lscc" || document.ChaincodeID == "_lifecycle" {
			continue
		}
		jsonBytes, err := json.Marshal(document.Data)
		if err != nil {
			return err
		}
		recordsToAdd = append(
			recordsToAdd,
			Record{
				ID:        document.PrimaryKey,
				Chaincode: document.ChaincodeID,
				Data:      jsonBytes,
			},
		)
		keyDocsAdded = append(keyDocsAdded, document.PrimaryKey)
	}
	m.db.Table(m.tableName).Clauses(clause.OnConflict{
		UpdateAll: true,
	}).CreateInBatches(recordsToAdd, 100)
	m.db.Table(m.tableName).Delete(Record{}, "id IN ?", recordsToRemove)

	log.Infof("Items added=%d %v", len(response.DocumentsToAdd), keyDocsAdded[:int(math.Min(float64(10), float64(len(keyDocsAdded))))])
	log.Infof("Items removed=%d", len(response.DocumentsToRemove))
	return nil
}

func (m DatabaseStorage) StoreBulk(blocks []*cb.Block) error {
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
func (m DatabaseStorage) Store(block *cb.Block) error {
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
