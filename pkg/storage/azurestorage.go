package storage

// Fork from Thanos S3 Bucket support to reuse configuration options
// Licensed under the Apache License 2.0
// https://github.com/thanos-io/thanos/blob/main/pkg/objstore/s3/s3.go

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/opencost/opencost/pkg/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"gopkg.in/yaml.v2"
)

const (
	azureDefaultEndpoint = "blob.core.windows.net"
)

// Set default retry values to default Azure values. 0 = use Default Azure.
var defaultAzureConfig = AzureConfig{
	PipelineConfig: PipelineConfig{
		MaxTries:      0,
		TryTimeout:    0,
		RetryDelay:    0,
		MaxRetryDelay: 0,
	},
	ReaderConfig: ReaderConfig{
		MaxRetryRequests: 0,
	},
	HTTPConfig: AzureHTTPConfig{
		IdleConnTimeout:       model.Duration(90 * time.Second),
		ResponseHeaderTimeout: model.Duration(2 * time.Minute),
		TLSHandshakeTimeout:   model.Duration(10 * time.Second),
		ExpectContinueTimeout: model.Duration(1 * time.Second),
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
		DisableCompression:    false,
	},
}

func init() {
	// Disable `ForceLog` in Azure storage module
	// As the time of this patch, the logging function in the storage module isn't correctly
	// detecting expected REST errors like 404 and so outputs them to syslog along with a stacktrace.
	// https://github.com/Azure/azure-storage-blob-go/issues/214
	//
	// This needs to be done at startup because the underlying variable is not thread safe.
	// https://github.com/Azure/azure-pipeline-go/blob/dc95902f1d32034f8f743ccc6c3f2eb36b84da27/pipeline/core.go#L276-L283
	pipeline.SetForceLogEnabled(false)
}

// AzureConfig Azure storage configuration.
type AzureConfig struct {
	StorageAccountName string          `yaml:"storage_account"`
	StorageAccountKey  string          `yaml:"storage_account_key"`
	ContainerName      string          `yaml:"container"`
	Endpoint           string          `yaml:"endpoint"`
	MaxRetries         int             `yaml:"max_retries"`
	MSIResource        string          `yaml:"msi_resource"`
	UserAssignedID     string          `yaml:"user_assigned_id"`
	PipelineConfig     PipelineConfig  `yaml:"pipeline_config"`
	ReaderConfig       ReaderConfig    `yaml:"reader_config"`
	HTTPConfig         AzureHTTPConfig `yaml:"http_config"`
}

type ReaderConfig struct {
	MaxRetryRequests int `yaml:"max_retry_requests"`
}

type PipelineConfig struct {
	MaxTries      int32          `yaml:"max_tries"`
	TryTimeout    model.Duration `yaml:"try_timeout"`
	RetryDelay    model.Duration `yaml:"retry_delay"`
	MaxRetryDelay model.Duration `yaml:"max_retry_delay"`
}

type AzureHTTPConfig struct {
	IdleConnTimeout       model.Duration `yaml:"idle_conn_timeout"`
	ResponseHeaderTimeout model.Duration `yaml:"response_header_timeout"`
	InsecureSkipVerify    bool           `yaml:"insecure_skip_verify"`

	TLSHandshakeTimeout   model.Duration `yaml:"tls_handshake_timeout"`
	ExpectContinueTimeout model.Duration `yaml:"expect_continue_timeout"`
	MaxIdleConns          int            `yaml:"max_idle_conns"`
	MaxIdleConnsPerHost   int            `yaml:"max_idle_conns_per_host"`
	MaxConnsPerHost       int            `yaml:"max_conns_per_host"`
	DisableCompression    bool           `yaml:"disable_compression"`

	TLSConfig TLSConfig `yaml:"tls_config"`
}

// AzureStorage implements the storage.Storage interface against Azure APIs.
type AzureStorage struct {
	name   string
	client *azblob.Client
	config *AzureConfig
}

// Validate checks to see if any of the config options are set.
func (conf *AzureConfig) validate() error {
	var errMsg []string
	if conf.MSIResource == "" {
		if conf.UserAssignedID == "" {
			if conf.StorageAccountName == "" {
				errMsg = append(errMsg, "invalid Azure storage configuration")
			}
		} else {
			if conf.StorageAccountName == "" {
				errMsg = append(errMsg, "UserAssignedID is configured but storage account name is missing")
			}
			if conf.StorageAccountKey != "" {
				errMsg = append(errMsg, "UserAssignedID is configured but storage account key is used")
			}
		}
	} else {
		if conf.StorageAccountName == "" {
			errMsg = append(errMsg, "MSI resource is configured but storage account name is missing")
		}
		if conf.StorageAccountKey != "" {
			errMsg = append(errMsg, "MSI resource is configured but storage account key is used")
		}
	}

	if conf.ContainerName == "" {
		errMsg = append(errMsg, "no Azure container specified")
	}
	if conf.Endpoint == "" {
		conf.Endpoint = azureDefaultEndpoint
	}

	if conf.PipelineConfig.MaxTries < 0 {
		errMsg = append(errMsg, "The value of max_tries must be greater than or equal to 0 in the config file")
	}

	if conf.ReaderConfig.MaxRetryRequests < 0 {
		errMsg = append(errMsg, "The value of max_retry_requests must be greater than or equal to 0 in the config file")
	}

	if len(errMsg) > 0 {
		return errors.New(strings.Join(errMsg, ", "))
	}

	return nil
}

// parseAzureConfig unmarshals a buffer into a Config with default values.
func parseAzureConfig(conf []byte) (AzureConfig, error) {
	config := defaultAzureConfig
	if err := yaml.UnmarshalStrict(conf, &config); err != nil {
		return AzureConfig{}, err
	}

	// If we don't have config specific retry values but we do have the generic MaxRetries.
	// This is for backwards compatibility but also ease of configuration.
	if config.MaxRetries > 0 {
		if config.PipelineConfig.MaxTries == 0 {
			config.PipelineConfig.MaxTries = int32(config.MaxRetries)
		}
		if config.ReaderConfig.MaxRetryRequests == 0 {
			config.ReaderConfig.MaxRetryRequests = config.MaxRetries
		}
	}

	return config, nil
}

// NewAzureStorage returns a new Storage using the provided Azure config.
func NewAzureStorage(azureConfig []byte) (*AzureStorage, error) {
	log.Debugf("Creating new Azure Bucket Connection")

	conf, err := parseAzureConfig(azureConfig)
	if err != nil {
		return nil, err
	}

	return NewAzureStorageWith(conf)
}

// NewAzureStorageWith returns a new Storage using the provided Azure config struct.
func NewAzureStorageWith(conf AzureConfig) (*AzureStorage, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("creating credential: %w", err)
	}

	client, err := azblob.NewClient(fmt.Sprintf("https://%s.blob.core.windows.net/", conf.StorageAccountName), cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	// TODO: retrieve container if creation fails
	_, err = client.CreateContainer(context.TODO(), conf.ContainerName, nil)
	if err != nil {
		if strings.Contains(err.Error(), "ContainerAlreadyExists") {
			log.Debugf("Container %s already exists", conf.ContainerName)
		} else {
			return nil, fmt.Errorf("creating container: %w", err)
		}
	}

	return &AzureStorage{
		name:   conf.ContainerName,
		client: client,
		config: &conf,
	}, nil
}

// Name returns the bucket name for azure storage.
func (as *AzureStorage) Name() string {
	return as.name
}

// StorageType returns a string identifier for the type of storage used by the implementation.
func (as *AzureStorage) StorageType() StorageType {
	return StorageTypeBucketAzure
}

// FullPath returns the storage working path combined with the path provided
func (as *AzureStorage) FullPath(name string) string {
	name = trimLeading(name)

	return name
}

// Stat returns the StorageStats for the specific path.
func (b *AzureStorage) Stat(name string) (*StorageInfo, error) {
	name = trimLeading(name)

	blobPages := b.client.NewListBlobsFlatPager(b.config.ContainerName, nil)

	for blobPages.More() {
		page, err := blobPages.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("getting service properties: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			if *blob.Name == name {
				return &StorageInfo{
					Name:    trimName(name),
					Size:    *blob.Properties.ContentLength,
					ModTime: *blob.Properties.LastModified,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("blob not found: %s", name)
}

// Read uses the relative path of the storage combined with the provided path to
// read the contents.
func (b *AzureStorage) Read(name string) ([]byte, error) {
	name = trimLeading(name)

	log.Debugf("AzureStorage::Read(%s)", name)

	info, err := b.Stat(name)
	if err != nil {
		return nil, fmt.Errorf("cannot stat Azure blob, blob: %s, err: %w", name, err)
	}

	buf := make([]byte, info.Size)
	_, err = b.client.DownloadBuffer(context.TODO(), b.config.ContainerName, name, buf, &azblob.DownloadBufferOptions{Range: blob.HTTPRange{Offset: 0, Count: info.Size}})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot download Azure blob, blob: %s", name)
	}

	return buf, nil
}

// Write uses the relative path of the storage combined with the provided path
// to write a new file or overwrite an existing file.
func (b *AzureStorage) Write(name string, data []byte) error {
	name = trimLeading(name)

	log.Debugf("AzureStorage::Write(%s)", name)

	_, err := b.client.UploadBuffer(context.TODO(), b.config.ContainerName, name, data, nil)
	if err != nil {
		return errors.Wrapf(err, "cannot upload Azure blob, address: %s", name)
	}
	return nil
}

// Remove uses the relative path of the storage combined with the provided path to
// remove a file from storage permanently.
func (b *AzureStorage) Remove(name string) error {
	name = trimLeading(name)

	log.Debugf("AzureStorage::Remove(%s)", name)

	_, err := b.client.DeleteBlob(context.TODO(), b.config.ContainerName, name, nil)
	if err != nil {
		return errors.Wrapf(err, "error deleting blob, address: %s", name)
	}

	return nil
}

// Exists uses the relative path of the storage combined with the provided path to
// determine if the file exists.
func (b *AzureStorage) Exists(name string) (bool, error) {

	_, err := b.Stat(name)
	if err != nil {
		return false, nil
	}

	return true, nil
}

// List uses the relative path of the storage combined with the provided path to return
// storage information for the files.
func (b *AzureStorage) List(path string) ([]*StorageInfo, error) {
	path = trimLeading(path)

	log.Debugf("AzureStorage::List(%s)", path)

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	var stats []*StorageInfo
	listOptions := &azblob.ListBlobsFlatOptions{Prefix: &path}
	blobPages := b.client.NewListBlobsFlatPager(b.config.ContainerName, listOptions)

	for blobPages.More() {
		page, err := blobPages.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("getting service properties: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			stats = append(stats, &StorageInfo{
				Name:    trimName(*blob.Name),
				Size:    *blob.Properties.ContentLength,
				ModTime: *blob.Properties.LastModified,
			})
		}
	}

	return stats, nil
}

func (b *AzureStorage) ListDirectories(path string) ([]*StorageInfo, error) {

	path = trimLeading(path)

	log.Debugf("AzureStorage::ListDirectories(%s)", path)

	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if path != "" {
		path = strings.TrimSuffix(path, DirDelim) + DirDelim
	}

	var stats []*StorageInfo
	listOptions := &azblob.ListBlobsFlatOptions{Prefix: &path}
	blobPages := b.client.NewListBlobsFlatPager(b.config.ContainerName, listOptions)

	for blobPages.More() {
		page, err := blobPages.NextPage(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("getting service properties: %w", err)
		}

		for _, blob := range page.Segment.BlobItems {
			stats = append(stats, &StorageInfo{
				Name:    trimName(*blob.Name),
				Size:    *blob.Properties.ContentLength,
				ModTime: *blob.Properties.LastModified,
			})
		}
	}

	return stats, nil
}

func DefaultAzureTransport(config AzureConfig) (*http.Transport, error) {
	tlsConfig, err := NewTLSConfig(&config.HTTPConfig.TLSConfig)
	if err != nil {
		return nil, err
	}

	if config.HTTPConfig.InsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,

		MaxIdleConns:          config.HTTPConfig.MaxIdleConns,
		MaxIdleConnsPerHost:   config.HTTPConfig.MaxIdleConnsPerHost,
		IdleConnTimeout:       time.Duration(config.HTTPConfig.IdleConnTimeout),
		MaxConnsPerHost:       config.HTTPConfig.MaxConnsPerHost,
		TLSHandshakeTimeout:   time.Duration(config.HTTPConfig.TLSHandshakeTimeout),
		ExpectContinueTimeout: time.Duration(config.HTTPConfig.ExpectContinueTimeout),

		ResponseHeaderTimeout: time.Duration(config.HTTPConfig.ResponseHeaderTimeout),
		DisableCompression:    config.HTTPConfig.DisableCompression,
		TLSClientConfig:       tlsConfig,
	}, nil
}
