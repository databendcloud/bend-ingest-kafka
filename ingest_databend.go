package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	godatabend "github.com/datafuselabs/databend-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/xitongsys/parquet-go-source/local"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

type DatabendIngester interface {
	IngestData(messageBatch *message.MessagesBatch) error
	IngestParquetData(messageBatch *message.MessagesBatch) error
	CreateRawTargetTable() error
}

type databendIngester struct {
	databendIngesterCfg *config.Config
	statsRecorder       *DatabendIngesterStatsRecorder
}

func NewDatabendIngester(cfg *config.Config) DatabendIngester {
	stats := NewDatabendIntesterStatsRecorder()
	return &databendIngester{
		databendIngesterCfg: cfg,
		statsRecorder:       stats,
	}
}

func (ig *databendIngester) reWriteTheJsonData(messagesBatch *message.MessagesBatch) ([]string, error) {
	batchJsonData := messagesBatch.Messages
	afterHandleJsonData := make([]string, 0, len(batchJsonData))

	for i := 0; i < len(batchJsonData); i++ {
		// re-write the json data into NDJson format, add the uuid, record_metadata and add_time fields
		recordMetadata := fmt.Sprintf("{\"topic\":\"%s\", \"partition\":%d,\"offset\":%d, \"key\":\"%s\", \"create_time\":\"%s\"}",
			ig.databendIngesterCfg.KafkaTopic,
			batchJsonData[i].Partition,
			batchJsonData[i].DataOffset,
			batchJsonData[i].Key,
			batchJsonData[i].CreateTime.Format(time.RFC3339Nano))
		// add the uuid, record_metadata and add_time fields
		d := fmt.Sprintf("{\"uuid\":\"%s\",\"koffset\":%d, \"kpartition\":%d, \"record_metadata\":%s,\"add_time\":\"%s\",\"raw_data\":%s}",
			uuid.New().String(),
			batchJsonData[i].DataOffset,
			batchJsonData[i].Partition,
			recordMetadata,
			time.Now().Format(time.RFC3339Nano),
			batchJsonData[i].Data)
		afterHandleJsonData = append(afterHandleJsonData, d)
	}
	return afterHandleJsonData, nil
}

func (ig *databendIngester) reWriteParquetJsonData(messagesBatch *message.MessagesBatch) ([]string, error) {
	batchJsonData := messagesBatch.Messages
	afterHandleJsonData := make([]string, 0, len(batchJsonData))

	for i := 0; i < len(batchJsonData); i++ {
		recordMetadata := fmt.Sprintf("{\"topic\":\"%s\", \"partition\":%d,\"offset\":%d, \"key\":\"%s\", \"create_time\":\"%s\"}",
			ig.databendIngesterCfg.KafkaTopic,
			batchJsonData[i].Partition,
			batchJsonData[i].DataOffset,
			batchJsonData[i].Key,
			batchJsonData[i].CreateTime.Format(time.RFC3339Nano))
		recordMetadataJson, err := json.Marshal(recordMetadata)
		if err != nil {
			return nil, err
		}
		dataJson, err := json.Marshal(batchJsonData[i].Data)
		if err != nil {
			return nil, err
		}
		// add the uuid, record_metadata and add_time fields
		d := fmt.Sprintf("{\"uuid\":\"%s\",\"koffset\":%d, \"kpartition\":%d, \"record_metadata\":%s,\"add_time\":\"%s\",\"raw_data\":%s}",
			uuid.New().String(),
			batchJsonData[i].DataOffset,
			batchJsonData[i].Partition,
			string(recordMetadataJson),
			time.Now().Format(time.RFC3339Nano),
			string(dataJson))
		afterHandleJsonData = append(afterHandleJsonData, d)
	}
	return afterHandleJsonData, nil
}

func (ig *databendIngester) IngestParquetData(messageBatch *message.MessagesBatch) error {
	l := logrus.WithFields(logrus.Fields{"ingest_databend": "IngestParquetData"})
	startTime := time.Now()
	if messageBatch == nil {
		return nil
	}
	batchJsonData := messageBatch.ExtractMessageData()

	if len(batchJsonData) == 0 {
		return nil
	}
	// handle batchJsonData, if isTransform is false, then the data is already in NDJson format
	// re-write the json data into NDJson format, add the uuid, record_metadata and add_time fields
	// then insert the data into the databend table
	var err error
	batchJsonData, err = ig.reWriteParquetJsonData(messageBatch)
	if err != nil {
		l.Errorf("re-write the json data failed: %v", err)
		return err
	}
	fileName, bytesSize, err := ig.generateParquetFile(batchJsonData)
	if err != nil {
		l.Errorf("generate parquet file failed: %v", err)
		return err
	}

	stage, err := ig.uploadToStage(fileName)
	if err != nil {
		l.Errorf("upload to stage failed: %v", err)
		return err
	}

	err = ig.replaceInto(stage)
	if err != nil {
		l.Errorf("replace into failed: %v", err)
		return err
	}

	ig.statsRecorder.RecordMetric(bytesSize, len(batchJsonData))
	stats := ig.statsRecorder.Stats(time.Since(startTime))
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s)", len(batchJsonData), stats.RowsPerSecondd, bytesSize, stats.BytesPerSecond)
	return nil
}

func (ig *databendIngester) IngestData(messageBatch *message.MessagesBatch) error {
	l := logrus.WithFields(logrus.Fields{"ingest_databend": "IngestData"})
	startTime := time.Now()
	if messageBatch == nil {
		return nil
	}
	batchJsonData := messageBatch.ExtractMessageData()

	if len(batchJsonData) == 0 {
		return nil
	}
	// handle batchJsonData, if isTransform is false, then the data is already in NDJson format
	// re-write the json data into NDJson format, add the uuid, record_metadata and add_time fields
	// then insert the data into the databend table
	if !ig.databendIngesterCfg.IsJsonTransform {
		var err error
		batchJsonData, err = ig.reWriteTheJsonData(messageBatch)
		if err != nil {
			l.Errorf("re-write the json data failed: %v", err)
			return err
		}
	}

	fileName, bytesSize, err := ig.generateNDJsonFile(batchJsonData)
	if err != nil {
		l.Errorf("generate NDJson file failed: %v", err)
		return err
	}

	stage, err := ig.uploadToStage(fileName)
	if err != nil {
		l.Errorf("upload to stage failed: %v", err)
		return err
	}

	err = ig.copyInto(stage)
	if err != nil {
		l.Errorf("copy into failed: %v", err)
		return err
	}
	ig.statsRecorder.RecordMetric(bytesSize, len(batchJsonData))
	stats := ig.statsRecorder.Stats(time.Since(startTime))
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s)", len(batchJsonData), stats.RowsPerSecondd, bytesSize, stats.BytesPerSecond)
	return nil
}

func (ig *databendIngester) generateParquetFile(batchJsonData []string) (string, int, error) {
	l := logrus.WithFields(logrus.Fields{"ingest_databend": "generateParquetFile"})

	tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("databend-ingest-%d-%s.parquet", time.Now().Unix(), uuid.New().String()))
	records := make([]*message.RecordForParquet, 0, len(batchJsonData))

	fw, err := local.NewLocalFileWriter(tmpFile)
	if err != nil {
		l.Errorf("Can't create local file writer: %v", err)
		return "", 0, err
	}

	pw, err := writer.NewParquetWriter(fw, new(message.RecordForParquet), 4)
	if err != nil {
		l.Errorf("Can't create parquet writer: %v", err)
		return "", 0, err
	}
	for _, data := range batchJsonData {
		var record message.RecordForParquet
		err := json.Unmarshal([]byte(data), &record)
		if err != nil {
			l.Errorf("Unmarshal json data failed: %v", err)
			return "", 0, err
		}
		records = append(records, &record)
	}

	for i := range records {
		if err = pw.Write(records[i]); err != nil {
			l.Errorf("Write record failed: %v", err)
			return "", 0, err
		}
	}
	if err = pw.WriteStop(); err != nil {
		l.Errorf("Write stop failed: %v", err)
		return "", 0, err
	}

	if err = fw.Close(); err != nil {
		l.Errorf("Close file writer failed: %v", err)
		return "", 0, err
	}
	fileSize := getFileSize(tmpFile)

	return tmpFile, fileSize, nil
}

func getFileSize(filename string) int {
	fi, err := os.Stat(filename)
	if err != nil {
		log.Fatal(err)
	}
	return int(fi.Size())
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

	databendConfig, err := godatabend.ParseDSN(ig.databendIngesterCfg.DatabendDSN)
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

	return stage, apiClient.UploadToStage(context.Background(), stage, input, size)
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
	copyIntoSQL := fmt.Sprintf("COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON missing_field_as = FIELD_DEFAULT COMPRESSION = AUTO) "+
		"PURGE = %v FORCE = %v DISABLE_VARIANT_CHECK = %v", ig.databendIngesterCfg.DatabendTable, stage.String(),
		ig.databendIngesterCfg.CopyPurge, ig.databendIngesterCfg.CopyForce, ig.databendIngesterCfg.DisableVariantCheck)
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	return execute(db, copyIntoSQL)
}

func (ig *databendIngester) replaceInto(stage *godatabend.StageLocation) error {
	replaceIntoSQL := fmt.Sprintf("REPLACE INTO %s ON (%s,%s) SELECT * FROM %s  (FILE_FORMAT => 'parquet')",
		ig.databendIngesterCfg.DatabendTable, "koffset", "kpartition", stage.String())
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	defer func() {
		if ig.databendIngesterCfg.CopyPurge {
			err := execute(db, fmt.Sprintf("REMOVE  %s", stage.String()))
			if err != nil {
				logrus.Errorf("remove stage file :%s failed: %v", stage.String(), err)
			}
		}
	}()
	err = execute(db, replaceIntoSQL)
	if err != nil {
		logrus.Errorf("replace into failed: %v", err)
		return err
	}
	return nil
}

func (ig *databendIngester) CreateRawTargetTable() error {
	// offset and partition is the key word of databend, so we need to use koffset and kpartition instead
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uuid String, koffset BIGINT, kpartition int, raw_data json, record_metadata json, add_time timestamp)", ig.databendIngesterCfg.DatabendTable)
	db, err := sql.Open("databend", ig.databendIngesterCfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	return execute(db, createTableSQL)
}
