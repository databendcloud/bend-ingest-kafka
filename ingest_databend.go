package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	godatabend "github.com/databendcloud/databend-go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type DatabendIngester interface {
	IngestData(batchJsonData []string) error
}

type databendIngester struct {
	databendDSN   string
	table         string
	statsRecorder *DatabendIngesterStatsRecorder
}

func NewDatabendIngester(dsn string, table string) DatabendIngester {
	stats := NewDatabendIntesterStatsRecorder()
	return &databendIngester{
		databendDSN:   dsn,
		table:         table,
		statsRecorder: stats,
	}
}

func (ig *databendIngester) IngestData(batchJsonData []string) error {
	startTime := time.Now()

	if len(batchJsonData) == 0 {
		return nil
	}

	fileName, bytesSize, err := ig.generateNDJsonFile(batchJsonData)
	if err != nil {
		return err
	}

	stage, err := ig.uploadToStage(fileName)
	if err != nil {
		return err
	}

	err = ig.copyInto(stage)
	if err != nil {
		return err
	}

	ig.statsRecorder.RecordMetric(bytesSize, len(batchJsonData))
	stats := ig.statsRecorder.Stats(time.Since(startTime))
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s)", len(batchJsonData), stats.RowsPerSecondd, bytesSize, stats.BytesPerSecond)
	return nil
}

func (ig *databendIngester) generateNDJsonFile(batchJsonData []string) (string, int, error) {
	outputFile, err := ioutil.TempFile("/tmp", "databend-ingest-*.ndjson")
	if err != nil {
		return "", 0, err
	}
	defer outputFile.Close()

	// Create a buffered writer for the Ndjson file
	writer := bufio.NewWriter(outputFile)
	bytesSum := 0

	for _, data := range batchJsonData {
		n, err := writer.WriteString(data + "\n")
		if err != nil {
			return "", 0, err
		}
		bytesSum += n
	}
	// Flush any remaining data to the NDJson file
	err = writer.Flush()
	if err != nil {
		return "", 0, err
	}
	return outputFile.Name(), bytesSum, err
}

func (ig *databendIngester) uploadToStage(fileName string) (*godatabend.StageLocation, error) {
	defer func() {
		err := os.RemoveAll(fileName)
		if err != nil {
			logrus.Errorf("delete batch insert file failed: %v", err)
		}
	}()

	databendConfig, err := godatabend.ParseDSN(ig.databendDSN)
	if err != nil {
		return nil, err
	}
	apiClient := godatabend.NewAPIClientFromConfig(databendConfig)
	fi, err := os.Stat(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "get batch file size failed")
	}
	size := fi.Size()

	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "open batch file failed")
	}
	defer f.Close()
	input := bufio.NewReader(f)
	stage := &godatabend.StageLocation{
		Name: "~",
		Path: fmt.Sprintf("batch/%d-%s", time.Now().Unix(), filepath.Base(fileName)),
	}

	return stage, apiClient.UploadToStage(stage, input, size)
}

func execute(db *sql.DB, sql string) error {
	_, err := db.Exec(sql)
	if err != nil {
		logrus.Errorf("exec '%s' failed, err: %v", sql, err)
		return err
	}
	return nil
}

func (ig *databendIngester) copyInto(stage *godatabend.StageLocation) error {
	copyIntoSQL := fmt.Sprintf("COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON)", ig.table, stage.String())
	db, err := sql.Open("databend", ig.databendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	return execute(db, copyIntoSQL)
}
