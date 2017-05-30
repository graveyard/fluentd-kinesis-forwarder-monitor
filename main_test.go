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

func TestParsePositionData(t *testing.T) {
	assert := assert.New(t)

	pos, err := parsePositionData([]byte("/a/file/path\t01\t03\n"))
	assert.NoError(err)
	assert.EqualValues(1, pos.offset)
	assert.EqualValues(3, pos.inode)

	pos, err = parsePositionData([]byte("/a/file/path\t01\t03"))
	assert.NoError(err)
	assert.EqualValues(1, pos.offset)
	assert.EqualValues(3, pos.inode)

	_, err = parsePositionData([]byte("/a/file/path\tpoo\t04\n"))
	assert.Error(err)

	_, err = parsePositionData([]byte("/a/file/path\t04\tpoo\n"))
	assert.Error(err)

	_, err = parsePositionData([]byte("\t04\t03\n"))
	assert.Error(err)
}
