package cmd

import (
	"strconv"
	"time"

	"github.com/kfsoftware/hlf-sync/pkg/listener"

	"github.com/dgraph-io/badger/v2"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-sdk-go/pkg/client/ledger"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/context"
	"github.com/hyperledger/fabric-sdk-go/pkg/common/providers/fab"
	"github.com/hyperledger/fabric-sdk-go/pkg/core/config"
	"github.com/hyperledger/fabric-sdk-go/pkg/fabsdk"
	"github.com/meilisearch/meilisearch-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Provider string

const (
	MeiliSearch   Provider = "meilisearch"
	ElasticSearch Provider = "elasticsearch"
	Database      Provider = "sql"
)

type options struct {
	configPath     string
	channelName    string
	org            string
	blockNumber    int
	chaincode      string
	batchIndexStep int
}

const (
	DataStoreDirectory = "hlf-sync.badgerdb"
	CurrentBlockKey    = "current_block"
	MaxBlockDistance   = 1
	BatchBlockIndexing = 2000
)

func getTargetPeers(ctxChannel context.Channel) ([]fab.Peer, error) {
	chHeight, err := getChannelHeight(ctxChannel)
	if err != nil {
		return nil, err
	}
	discovery, err := ctxChannel.ChannelService().Discovery()
	if err != nil {
		return nil, err
	}
	peers, err := discovery.GetPeers()
	if err != nil {
		return nil, err
	}
	var targetPeers []fab.Peer
	for _, peer := range peers {
		props := peer.Properties()
		peerHeight := int(props[fab.PropertyLedgerHeight].(uint64))
		if chHeight-peerHeight < 1000 {
			targetPeers = append(targetPeers, peer)
		}
	}
	return targetPeers, nil
}
func getChannelHeight(ctxChannel context.Channel) (int, error) {
	discovery, err := ctxChannel.ChannelService().Discovery()
	if err != nil {
		return 0, err
	}
	peers, err := discovery.GetPeers()
	if err != nil {
		return 0, err
	}
	ledgerHeight := 0
	for _, peer := range peers {
		props := peer.Properties()
		peerHeight := int(props[fab.PropertyLedgerHeight].(uint64))
		if peerHeight > ledgerHeight {
			ledgerHeight = peerHeight - 1
		}
	}
	log.Infof("Ledger height= %d", ledgerHeight)
	return ledgerHeight, nil
}
func NewSyncCmd() *cobra.Command {
	c := options{}
	cmd := &cobra.Command{
		Use: "sync",
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := badger.DefaultOptions(DataStoreDirectory)

			db, err := badger.Open(opts)
			if err != nil {
				return err
			}
			provider := viper.GetString("database.type")
			var storage listener.BlockStorage
			switch provider {
			case string(MeiliSearch):
				databaseURL := viper.GetString("database.url")
				password := viper.GetString("database.apiKey")
				meiliClient := meilisearch.NewClient(meilisearch.Config{
					Host:   databaseURL,
					APIKey: password,
				})
				_, err := meiliClient.Indexes().List()
				if err != nil {
					return err
				}
				storage, err = listener.NewMeilisearchStorage(meiliClient, c.channelName)
				if err != nil {
					return err
				}
			case string(ElasticSearch):
				databaseURLs := viper.GetStringSlice("database.urls")
				user := viper.GetString("database.user")
				password := viper.GetString("database.password")
				cfg := elasticsearch.Config{
					Addresses: databaseURLs,
					Username:  user,
					Password:  password,
				}
				esClient, err := elasticsearch.NewClient(
					cfg,
				)
				if err != nil {
					log.Fatalf("Error creating the client: %s", err)
					return err
				}
				storage = listener.NewElasticStorage(esClient)
			case string(Database):
				driverName := viper.GetString("database.driver")
				dataSource := viper.GetString("database.dataSource")
				var drName listener.DriverName
				switch driverName {
				case listener.PostgresqlDriver:
					drName = listener.PostgresqlDriver
				case listener.MySQLDriver:
					drName = listener.MySQLDriver
				default:
					return errors.Errorf("Driver %s not supported", driverName)
				}
				storage, err = listener.NewPostgresStorage(
					drName,
					dataSource,
					c.channelName,
				)
				if err != nil {
					return err
				}
			default:
				return errors.Errorf("No valid provider: %s", provider)
			}
			configBackend := config.FromFile(c.configPath)
			sdk, err := fabsdk.New(configBackend)
			if err != nil {
				return err
			}
			channelCtx := sdk.ChannelContext(
				c.channelName,
				fabsdk.WithUser("admin"),
				fabsdk.WithOrg(c.org),
			)
			var blockNumber int
			err = db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte(CurrentBlockKey))
				if err != nil {
					log.Warnf("Entry, listening from first block: %v", err)
					blockNumber = 0
					return err
				}
				val, err := item.ValueCopy(nil)
				if err != nil {
					log.Warnf("Block number not found, listening from first block: %v", err)
					blockNumber = 0
					return err
				}
				blockNumberStored, err := strconv.Atoi(string(val))
				if err != nil {
					log.Warnf("Block number not found, listening from first block: %v", err)
					blockNumber = 0
				} else {
					blockNumber = blockNumberStored + 1
					log.Infof("Block number found: %d", blockNumber)
				}
				return nil
			})
			if c.blockNumber >= 0 {
				blockNumber = c.blockNumber
			}
			ledgerClient, err := ledger.New(channelCtx)
			if err != nil {
				return err
			}
			chCtx, err := channelCtx()
			if err != nil {
				return err
			}
			targetPeers, err := getTargetPeers(chCtx)
			if err != nil {
				return err
			}
			log.Infof("Peers %v", targetPeers)
			chHeightBlock, err := getChannelHeight(chCtx)
			if err != nil {
				return err
			}
			if chHeightBlock-blockNumber > MaxBlockDistance {
				log.Infof("Starting bulk indexing, distance is=%d", chHeightBlock-blockNumber)
				// for {
				// 	chHeightBlock, err = getChannelHeight(chCtx)
				// 	if err != nil {
				// 		return err
				// 	}
				// 	nextBatchBlockNumber := blockNumber + c.batchIndexStep
				// 	nextMaxBlock := int(math.Min(float64(chHeightBlock), float64(nextBatchBlockNumber)))
				// 	log.Debugf("Next max block %d, current block=%d, height=%d", nextMaxBlock, blockNumber, chHeightBlock)
				// 	if blockNumber >= chHeightBlock {
				// 		log.Debugf("Skipping, since next block %d and height=%d", blockNumber, chHeightBlock)
				// 		break
				// 	}
				// 	// var blocks []*common.Block
				// 	// for i := blockNumber; i <= nextMaxBlock; i++ {
				// 	// 	block, err := ledgerClient.QueryBlock(uint64(i), ledger.WithTargets(targetPeers...))
				// 	// 	if err != nil {
				// 	// 		return err
				// 	// 	}
				// 	// 	if i%100 == 0 {
				// 	// 		log.Infof("Fetching %d from %d", i, nextMaxBlock)
				// 	// 	}
				// 	// 	// log.Debugf("Fetching block %d", i)
				// 	// 	blocks = append(blocks, block)
				// 	// }
				// 	// log.Debugf("Blocks in bulk=%d", len(blocks))
				// 	// err = storage.StoreBulk(blocks)
				// 	// if err != nil {
				// 	// 	return err
				// 	// }
				// 	// log.Debugf("Updating database for block numbers=%d..%d", blockNumber, int(nextMaxBlock))
				// 	// blockNumber = nextMaxBlock
				// 	// err = db.Update(func(txn *badger.Txn) error {
				// 	// 	val := []byte(strconv.Itoa(blockNumber))
				// 	// 	err = txn.Set([]byte(CurrentBlockKey), val)
				// 	// 	if err != nil {
				// 	// 		log.Errorf("Failed to set current block key=%v", err)
				// 	// 	}
				// 	// 	return err
				// 	// })
				// 	// if err != nil {
				// 	// 	log.Errorf("Failed to update Badger Database %v", err)
				// 	// }
				// }
			}
			log.Infof("Starting from block number: %d", blockNumber)
			pause := 10 * time.Second
			go func() {
				for {
					currHeight, err := getChannelHeight(chCtx)
					if err != nil {
						log.Fatalf("Failed getting blockchain info: %v", err)
						return
					}
					if currHeight == blockNumber {
						log.Infof("There are no blocks created, sleeping for %s", pause)
						time.Sleep(pause)
						continue
					}
					var blocks []*common.Block
					for i := blockNumber; i <= currHeight; i++ {
						block, err := ledgerClient.QueryBlock(uint64(i), ledger.WithTargets(targetPeers...))
						if err != nil {
							log.Fatalf("Failed getting block %d: %v", i, err)
							return
						}
						if i%100 == 0 {
							log.Infof("Fetching %d from %d", i, currHeight)
						}
						blocks = append(blocks, block)
					}
					log.Debugf("Blocks in bulk=%d", len(blocks))
					err = storage.StoreBulk(blocks)
					if err != nil {
						log.Fatalf("Failed storing %d blocks: %v", len(blocks), err)
						return
					}
					log.Debugf("Updating database for block numbers=%d..%d", blockNumber, currHeight)
					blockNumber = currHeight
					err = db.Update(func(txn *badger.Txn) error {
						val := []byte(strconv.Itoa(blockNumber))
						err = txn.Set([]byte(CurrentBlockKey), val)
						if err != nil {
							log.Errorf("Failed to set current block key=%v", err)
						}
						return err
					})
					if err != nil {
						log.Errorf("Failed to update Badger Database %v", err)
					}
					log.Infof("Sleeping for %s..", pause)
					time.Sleep(pause)
				}

			}()
			select {}
		},
	}

	persistentFlags := cmd.PersistentFlags()
	persistentFlags.StringVarP(&c.configPath, "config", "", "", "Configuration file for the SDK")
	persistentFlags.StringVarP(&c.channelName, "channel", "", "", "Configuration file for the SDK")
	persistentFlags.StringVarP(&c.org, "org", "", "", "Configuration file for the SDK")
	persistentFlags.IntVarP(&c.batchIndexStep, "batch-index", "", BatchBlockIndexing, "Number of blocks per batch")
	persistentFlags.IntVarP(&c.blockNumber, "block-number", "", -1, "Configuration file for the SDK")
	cmd.MarkPersistentFlagRequired("config")
	cmd.MarkPersistentFlagRequired("channel")
	cmd.MarkPersistentFlagRequired("org")
	return cmd
}
