package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"

	corev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/core/v1"
	typev1 "buf.build/gen/go/twisp/api/protocolbuffers/go/twisp/type/v1"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbv1 "github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/uuid"
	"github.com/parsnips/twisp-util/pkg/util"
	"google.golang.org/protobuf/proto"
)

var (
	table    *string
	endpoint *string
	client   *dynamodb.Client
)

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

const (
	defaultAccount = "000000000000"
	defaultRegion  = "us-west-2"
)

func main() {
	fromTwispAccount := flag.String("from-twisp-account", defaultAccount, "the x-twisp-account-id you want to restore from.")
	fromRegion := flag.String("from-region", defaultRegion, "The region you want to restore to.")
	toTwispAccount := flag.String("to-twisp-account", defaultAccount, "The x-twisp-account-id you want to restore to..")
	toRegion := flag.String("to-region", defaultRegion, "The region you want to restore to.")
	file := flag.String("file", "backup.jsonl", `the backup file to restore from. A jsonl file of dynamodb json {"Item":{}}`)
	table = flag.String("table", "0a5ccc1d-7ac0-4efb-818b-d845b3a82165", "The dynamodb table to write to.")
	endpoint = flag.String("endpoint", "", "Optional endpoint to set the dynamodb client to.")
	flag.Parse()

	// Setup the client
	cfg, err := config.LoadDefaultConfig(context.Background())
	handleErr(err)
	if *endpoint != "" {
		cfg.Region = "us-west-2"
		cfg.Credentials = credentials.NewStaticCredentialsProvider("key", "secret", "")
		cfg.EndpointResolver = &util.LocalResolver{URL: *endpoint}
	}
	client = dynamodb.NewFromConfig(cfg)

	// Open the backup file
	f, err := os.Open(*file)
	if err != nil {
		handleErr(err)
	}
	defer f.Close()
	accountUUIDSpace := uuid.MustParse("0cf49e6e-ec7e-4c81-b7ba-a984b8db762a")
	journalTableId := uuid.MustParse("9233c407-a3ab-4277-9813-ddd40582bfcd")

	// From tenant
	fromArn := arn.ARN{
		Partition: "twisp",
		Service:   "database",
		Region:    *fromRegion,
		AccountID: *fromTwispAccount,
	}
	fromTenantId := uuid.NewSHA1(
		accountUUIDSpace,
		[]byte(fromArn.String()),
	)

	// To tenant
	toArn := arn.ARN{
		Partition: "twisp",
		Service:   "database",
		Region:    *toRegion,
		AccountID: *toTwispAccount,
	}
	toTenantId := uuid.NewSHA1(
		accountUUIDSpace,
		[]byte(toArn.String()),
	)

	zedUUID := &typev1.UUID{}

	isDefaultJournalRecord := func(raw RawData) bool {
		if item, ok := raw.Item["t"]; ok && bytes.Equal(item.B[:], journalTableId[:]) {
			var journal corev1.Journal
			if err := proto.Unmarshal(raw.Item["g"].B, &journal); err != nil {
				handleErr(err)
			}
			return proto.Equal(zedUUID, journal.JournalId)
		}

		return false
	}

	// Filter if from tenant
	isFromTenant := func(raw RawData) bool {
		item, ok := raw.Item["a"]
		return ok && len(item.B) > 16 && bytes.Equal(item.B[:16], fromTenantId[:])
	}

	// function to change the tenantId if region or account is different
	convertIntoToTenant := func(raw RawData) {
		item, ok := raw.Item["a"]
		if ok && len(item.B) > 16 {
			copy(raw.Item["a"].B[:16], toTenantId[:])
		}
	}

	// Spin up worker infra
	workerCount := MaxParallelism()
	requests := make(chan RawData)
	done := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			buffer := []RawData{}
			for {
				select {
				case req := <-requests:
					if len(buffer) == 25 {
						if err := batchPut(buffer); err != nil {
							fmt.Fprintf(os.Stderr, "%s\n", err)
						}
						buffer = []RawData{}
					}
					buffer = append(buffer, req)
				case <-done:
					if len(buffer) > 0 {
						if err := batchPut(buffer); err != nil {
							os.Stderr.Write([]byte(err.Error() + "\n"))
						}
					}
					return
				}
			}
		}()
	}

	// Scan the file and send requests to workers
	go func() {
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var raw RawData
			if err := json.Unmarshal(scanner.Bytes(), &raw); err != nil {
				handleErr(err)
			}
			if isFromTenant(raw) {
				if isDefaultJournalRecord(raw) {
					// TODO: Note rowID and remove index records with this row id
					continue
				}
				convertIntoToTenant(raw)
				requests <- raw
			}
		}
		close(done)
	}()

	// Wait for all workers to finish
	wg.Wait()
}

func batchPut(raw []RawData) error {
	requests, err := util.MapOrError(raw, func(val RawData, _ int) (types.WriteRequest, error) {
		asEvent, err := util.ToV2AttributeValueMap(val.Item)
		if err != nil {
			return types.WriteRequest{}, err
		}
		return types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: asEvent,
			},
		}, nil
	})
	if err != nil {
		return err
	}

	resp, err := client.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			*table: requests,
		},
	})
	if err != nil {
		return err
	}
	if len(resp.UnprocessedItems) > 0 {
		b, err := json.Marshal(resp.UnprocessedItems)
		if err != nil {
			return err
		}
		os.Stderr.Write([]byte(string(b) + "\n"))
	}
	return err
}

func handleErr(err error) {
	if err != nil {
		os.Stderr.Write([]byte(err.Error() + "\n"))
		os.Exit(1)
	}
}

type RawData struct {
	Item map[string]*dynamodbv1.AttributeValue
}
