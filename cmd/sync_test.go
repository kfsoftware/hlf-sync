package cmd

import (
	"bytes"
	"testing"
)

func Test_ExecuteCommand(t *testing.T) {
	cmd := NewSyncCmd()
	b := bytes.NewBufferString("")
	cmd.SetOut(b)
	cmd.Execute()
}
