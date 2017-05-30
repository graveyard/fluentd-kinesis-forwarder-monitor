package main

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadLine(t *testing.T) {
	assert := assert.New(t)

	line, err := readLine("./test-files/messages.logs", 10)
	assert.NoError(err)
	assert.Equal(line, "this is line starts on the 10th byte")

	line, err = readLine("./test-files/messages.logs", 100)
	assert.Error(err)
	assert.Equal(err, io.EOF)

	line, err = readLine("./test-files/messages.logs.nofile", 10)
	assert.Error(err)
}
