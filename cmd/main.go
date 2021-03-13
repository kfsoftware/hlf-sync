package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:    "hlf-sync",
	Short:  "HLF sync",
	Long:   `HLF sync is a tool to store all the transaction data of Hyperledger Fabric into a database`,
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		//cmd.AddCommand(syncCmd)
	},
}

func Execute() {
	rootCmd.AddCommand(NewSyncCmd())
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
