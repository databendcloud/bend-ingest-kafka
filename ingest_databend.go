package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	godatabend "github.com/datafuselabs/databend-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/xitongsys/parquet-go-source/local"

	"github.com/databendcloud/bend-ingest-kafka/config"
	"github.com/databendcloud/bend-ingest-kafka/message"
)

var (
	ErrUploadStageFailed = errors.New("upload stage failed")
	ErrCopyIntoFailed    = errors.New("copy into failed")
)

type DatabendIngester interface {
	IngestData(messageBatch *message.MessagesBatch) error
	IngestParquetData(messageBatch *message.MessagesBatch) error
	CreateRawTargetTable() error
	Close() error
}

type databendIngester struct {
	databendIngesterCfg *config.Config
	statsRecorder       *DatabendIngesterStatsRecorder
	db                  *sql.DB
}

type kafkaRecordMetadata struct {
	Topic       string `json:"topic"`
	Partition   int    `json:"partition"`
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	KeyEncoding string `json:"key_encoding,omitempty"`
	CreateTime  string `json:"create_time"`
}

type ndjsonRecord struct {
	UUID           string              `json:"uuid"`
	Koffset        int64               `json:"koffset"`
	Kpartition     int                 `json:"kpartition"`
	RecordMetadata kafkaRecordMetadata `json:"record_metadata"`
	AddTime        string              `json:"add_time"`
	RawData        json.RawMessage     `json:"raw_data"`
}

func NewDatabendIngester(cfg *config.Config) DatabendIngester {
	stats := NewDatabendIntesterStatsRecorder()
	db, err := sql.Open("databend", cfg.DatabendDSN)
	if err != nil {
		log.Fatalf("open databend failed: %v", err)
	}
	return &databendIngester{
		databendIngesterCfg: cfg,
		statsRecorder:       stats,
		db:                  db,
	}
}

func (ig *databendIngester) Close() error {
	if ig.db != nil {
		return ig.db.Close()
	}
	return nil
}

// sanitizeUTF8 removes or replaces invalid UTF-8 sequences from a string
func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}

	// Method 1: Use strings.ToValidUTF8 to replace invalid sequences with empty string
	// This is the most reliable approach using Go standard library
	var b strings.Builder
	b.Grow(len(s))

	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8 byte, skip it
			s = s[1:]
			continue
		}
		b.WriteRune(r)
		s = s[size:]
	}

	return b.String()
}

func sanitizeKafkaKey(key string) (string, string) {
	if key == "" {
		return "", ""
	}
	if utf8.ValidString(key) {
		return key, ""
	}
	return base64.StdEncoding.EncodeToString([]byte(key)), "base64"
}

func prepareRawData(data string, offset int64) (json.RawMessage, error) {
	payload := data
	if !utf8.ValidString(payload) {
		payload = sanitizeUTF8(payload)
	}

	raw := bytes.TrimSpace([]byte(payload))
	if len(raw) == 0 {
		return json.RawMessage([]byte("null")), nil
	}

	if !json.Valid(raw) {
		return nil, fmt.Errorf("invalid json payload at offset %d", offset)
	}

	copied := make([]byte, len(raw))
	copy(copied, raw)
	return json.RawMessage(copied), nil
}

func (ig *databendIngester) buildNDJSONRecord(msg message.MessageData) (*ndjsonRecord, error) {
	rawData, err := prepareRawData(msg.Data, msg.DataOffset)
	if err != nil {
		return nil, err
	}
	keyValue, encoding := sanitizeKafkaKey(msg.Key)

	record := &ndjsonRecord{
		UUID:       uuid.New().String(),
		Koffset:    msg.DataOffset,
		Kpartition: msg.Partition,
		RecordMetadata: kafkaRecordMetadata{
			Topic:       ig.databendIngesterCfg.KafkaTopic,
			Partition:   msg.Partition,
			Offset:      msg.DataOffset,
			Key:         keyValue,
			KeyEncoding: encoding,
			CreateTime:  msg.CreateTime.Format(time.RFC3339Nano),
		},
		AddTime: time.Now().Format(time.RFC3339Nano),
		RawData: rawData,
	}
	return record, nil
}

func (ig *databendIngester) reWriteTheJsonData(messagesBatch *message.MessagesBatch) ([]string, error) {
	batchJsonData := messagesBatch.Messages
	afterHandleJsonData := make([]string, 0, len(batchJsonData))

	for i := 0; i < len(batchJsonData); i++ {
		record, err := ig.buildNDJSONRecord(batchJsonData[i])
		if err != nil {
			return nil, err
		}

		jsonData, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal record: %w", err)
		}

		afterHandleJsonData = append(afterHandleJsonData, string(jsonData))
	}
	return afterHandleJsonData, nil
}

func (ig *databendIngester) reWriteParquetJsonData(messagesBatch *message.MessagesBatch) ([]string, error) {
	batchJsonData := messagesBatch.Messages
	afterHandleJsonData := make([]string, 0, len(batchJsonData))

	for i := 0; i < len(batchJsonData); i++ {
		record, err := ig.buildNDJSONRecord(batchJsonData[i])
		if err != nil {
			return nil, err
		}

		metadataJSON, err := json.Marshal(record.RecordMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal record metadata: %w", err)
		}

		parquetRecord := message.RecordForParquet{
			UUID:           record.UUID,
			KOffset:        record.Koffset,
			KPartition:     int32(record.Kpartition),
			RecordMetadata: string(metadataJSON),
			AddTime:        record.AddTime,
			RawData:        string(record.RawData),
		}

		jsonData, err := json.Marshal(parquetRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal parquet record: %w", err)
		}

		afterHandleJsonData = append(afterHandleJsonData, string(jsonData))
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
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s)", len(batchJsonData), stats.RowsPerSecond, bytesSize, stats.BytesPerSecond)
	return nil
}

func (ig *databendIngester) IngestData(messageBatch *message.MessagesBatch) error {
	l := logrus.WithFields(logrus.Fields{"ingest_databend": "IngestData", "lastOffset": messageBatch.LastMessageOffset})
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
			l.Errorf("re-write the json data failed: %v, lastOffset is: %d\n", err, messageBatch.LastMessageOffset)
			return err
		}
	}

	fileName, bytesSize, err := ig.generateNDJsonFile(batchJsonData)
	if err != nil {
		l.Errorf("generate NDJson file failed: %v,lastOffset is %d\n", err, messageBatch.LastMessageOffset)
		return err
	}

	stage, err := ig.uploadToStage(fileName)
	if err != nil {
		l.Errorf("upload to stage failed: %v, lastOffset is: %d, partition is %d\n", err,
			messageBatch.LastMessageOffset, messageBatch.Messages[0].Partition)
		return err
	}

	err = ig.copyInto(stage)
	if err != nil {
		l.Errorf("copy into failed: %v, lastOffset is: %d, partition is %d\n", err,
			messageBatch.LastMessageOffset, messageBatch.Messages[0].Partition)
		return err
	}
	ig.statsRecorder.RecordMetric(bytesSize, len(batchJsonData))
	stats := ig.statsRecorder.Stats(time.Since(startTime))
	log.Printf("ingest %d rows (%f rows/s), %d bytes (%f bytes/s) in %s", len(batchJsonData), stats.RowsPerSecond, bytesSize, stats.BytesPerSecond, time.Since(startTime))
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
	startTime := time.Now()
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
		Name: ig.databendIngesterCfg.UserStage,
		Path: fmt.Sprintf("batch/%d-%s", time.Now().Unix(), filepath.Base(fileName)),
	}

	if err := apiClient.UploadToStage(context.Background(), stage, input, size); err != nil {
		return nil, errors.Wrap(ErrUploadStageFailed, err.Error())
	}

	logrus.Infof("upload to stage %s, cost: %s", stage.String(), time.Since(startTime))
	return stage, nil
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
	startTime := time.Now()
	copyIntoSQL := fmt.Sprintf("COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON missing_field_as = FIELD_DEFAULT COMPRESSION = AUTO) "+
		"PURGE = %v FORCE = %v DISABLE_VARIANT_CHECK = %v", ig.databendIngesterCfg.DatabendTable, stage.String(),
		ig.databendIngesterCfg.CopyPurge, ig.databendIngesterCfg.CopyForce, ig.databendIngesterCfg.DisableVariantCheck)

	if err := execute(ig.db, copyIntoSQL); err != nil {
		return errors.Wrap(ErrCopyIntoFailed, err.Error())
	}
	logrus.Infof("copy into %s, cost: %s", ig.databendIngesterCfg.DatabendTable, time.Since(startTime))
	return nil
}

func (ig *databendIngester) replaceInto(stage *godatabend.StageLocation) error {
	replaceIntoSQL := fmt.Sprintf("REPLACE INTO %s ON (%s,%s) SELECT * FROM %s  (FILE_FORMAT => 'parquet')",
		ig.databendIngesterCfg.DatabendTable, "koffset", "kpartition", stage.String())

	defer func() {
		if ig.databendIngesterCfg.CopyPurge {
			err := execute(ig.db, fmt.Sprintf("REMOVE  %s", stage.String()))
			if err != nil {
				logrus.Errorf("remove stage file :%s failed: %v", stage.String(), err)
			}
		}
	}()
	err := execute(ig.db, replaceIntoSQL)
	if err != nil {
		logrus.Errorf("replace into failed: %v", err)
		return err
	}
	return nil
}

func (ig *databendIngester) CreateRawTargetTable() error {
	// offset and partition is the key word of databend, so we need to use koffset and kpartition instead
	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (uuid String, koffset BIGINT, kpartition int, raw_data json, record_metadata json, add_time timestamp)", ig.databendIngesterCfg.DatabendTable)

	return execute(ig.db, createTableSQL)
}
