package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	godatabend "github.com/databendcloud/databend-go"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type Ingester interface {
	IngestData(batchJsonData []string) error
	GenerateNDJsonFile(batchJsonData []string) (string, error)
	UploadToStage(fileName string) (*godatabend.StageLocation, error)
	CopyInto(stage *godatabend.StageLocation) error
}

func (cfg *Config) IngestData(batchJsonData []string) error {
	if len(batchJsonData) == 0 {
		return nil
	}
	fileName, err := cfg.GenerateNDJsonFile(batchJsonData)
	if err != nil {
		return err
	}

	stage, err := cfg.UploadToStage(fileName)
	if err != nil {
		return err
	}

	err = cfg.CopyInto(stage)
	if err != nil {
		return err
	}

	return nil
}

func (cfg *Config) GenerateNDJsonFile(batchJsonData []string) (string, error) {
	randomNDJsonFileName := fmt.Sprintf("%s.ndjson", uuid.NewString())
	outputFile, err := os.OpenFile(randomNDJsonFileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer outputFile.Close()

	// Create a buffered writer for the Ndjson file
	writer := bufio.NewWriter(outputFile)

	for _, data := range batchJsonData {
		_, err = writer.WriteString(data + "\n")
		if err != nil {
			return "", err
		}
	}
	// Flush any remaining data to the NDJson file
	err = writer.Flush()
	if err != nil {
		return "", err
	}
	return randomNDJsonFileName, err
}

func (cfg *Config) UploadToStage(fileName string) (*godatabend.StageLocation, error) {
	defer func() {
		err := os.RemoveAll(fileName)
		if err != nil {
			logrus.Errorf("delete batch insert file failed: %v", err)
		}
	}()

	databendConfig, err := godatabend.ParseDSN(cfg.DatabendDSN)
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

func (cfg *Config) CopyInto(stage *godatabend.StageLocation) error {
	copyIntoSQL := fmt.Sprintf("COPY INTO %s FROM %s FILE_FORMAT = (type = NDJSON)", cfg.DatabendTable, stage.String())
	db, err := sql.Open("databend", cfg.DatabendDSN)
	if err != nil {
		logrus.Errorf("create db error: %v", err)
		return err
	}
	return execute(db, copyIntoSQL)
}
