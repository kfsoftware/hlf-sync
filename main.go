package main

import (
	"fmt"

	"github.com/kfsoftware/hlf-sync/cmd"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func main() {
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)

	viper.SetConfigName("hlf_sync")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("hlf")
	viper.AddConfigPath(".") // optionally look for config in the working directory

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	log.SetLevel(log.DebugLevel)
	cmd.Execute()
}
