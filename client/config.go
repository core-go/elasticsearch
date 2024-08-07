package client

import (
	"crypto/tls"
	"github.com/elastic/go-elasticsearch/v8"
	"net"
	"net/http"
	"time"
)

type TransportConfig struct {
	MaxIdleConnsPerHost   *int   `yaml:"max_idle_conns_per_host" mapstructure:"max_idle_conns_per_host" json:"maxIdleConnsPerHost,omitempty" gorm:"column:maxidleconnsperhost" bson:"maxIdleConnsPerHost,omitempty" dynamodbav:"maxIdleConnsPerHost,omitempty" firestore:"maxIdleConnsPerHost,omitempty"`
	ResponseHeaderTimeout *int64 `yaml:"response_header_timeout" mapstructure:"response_header_timeout" json:"responseHeaderTimeout,omitempty" gorm:"column:responseheadertimeout" bson:"responseHeaderTimeout,omitempty" dynamodbav:"responseHeaderTimeout,omitempty" firestore:"responseHeaderTimeout,omitempty"`
	Timeout               *int64 `yaml:"timeout" mapstructure:"timeout" json:"timeout,omitempty" gorm:"column:timeout" bson:"timeout,omitempty" dynamodbav:"timeout,omitempty" firestore:"timeout,omitempty"`
}
type Config struct {
	Addresses    []string `yaml:"addresses" mapstructure:"addresses" json:"addresses,omitempty" gorm:"column:addresses" bson:"addresses,omitempty" dynamodbav:"addresses,omitempty" firestore:"addresses,omitempty"`
	Username     *string  `yaml:"username" mapstructure:"username" json:"username,omitempty" gorm:"column:username" bson:"username,omitempty" dynamodbav:"username,omitempty" firestore:"username,omitempty"`
	Password     *string  `yaml:"password" mapstructure:"password" json:"password,omitempty" gorm:"column:password" bson:"password,omitempty" dynamodbav:"password,omitempty" firestore:"password,omitempty"`
	CloudID      *string  `yaml:"cloud_id" mapstructure:"cloud_id" json:"cloudID,omitempty" gorm:"column:cloudid" bson:"cloudID,omitempty" dynamodbav:"cloudID,omitempty" firestore:"cloudID,omitempty"`
	APIKey       *string  `yaml:"api_key" mapstructure:"api_key" json:"apiKey,omitempty" gorm:"column:apikey" bson:"apiKey,omitempty" dynamodbav:"apiKey,omitempty" firestore:"apiKey,omitempty"`
	DisableRetry *bool    `yaml:"disable_retry" mapstructure:"disable_retry" json:"disableRetry,omitempty" gorm:"column:disableretry" bson:"disableRetry,omitempty" dynamodbav:"disableRetry,omitempty" firestore:"disableRetry,omitempty"`
	// EnableRetryOnTimeout  *bool           `yaml:"enable_retry_on_timeout" mapstructure:"enable_retry_on_timeout" json:"enableRetryOnTimeout,omitempty" gorm:"column:enableretryontimeout" bson:"enableRetryOnTimeout,omitempty" dynamodbav:"enableRetryOnTimeout,omitempty" firestore:"enableRetryOnTimeout,omitempty"`
	MaxRetries            *int            `yaml:"max_retries" mapstructure:"max_retries" json:"maxRetries,omitempty" gorm:"column:maxretries" bson:"maxRetries,omitempty" dynamodbav:"maxRetries,omitempty" firestore:"maxRetries,omitempty"`
	DiscoverNodesOnStart  *bool           `yaml:"discover_nodes_on_start" mapstructure:"discover_nodes_on_start" json:"discoverNodesOnStart,omitempty" gorm:"column:discovernodesonstart" bson:"discoverNodesOnStart,omitempty" dynamodbav:"discoverNodesOnStart,omitempty" firestore:"discoverNodesOnStart,omitempty"`
	DiscoverNodesInterval *int64          `yaml:"discover_nodes_interval" mapstructure:"discover_nodes_interval" json:"discoverNodesInterval,omitempty" gorm:"column:discovernodesinterval" bson:"discoverNodesInterval,omitempty" dynamodbav:"discoverNodesInterval,omitempty" firestore:"discoverNodesInterval,omitempty"`
	EnableMetrics         *bool           `yaml:"enable_metrics" mapstructure:"enable_metrics" json:"enableMetrics,omitempty" gorm:"column:enablemetrics" bson:"enableMetrics,omitempty" dynamodbav:"enableMetrics,omitempty" firestore:"enableMetrics,omitempty"`
	EnableDebugLogger     *bool           `yaml:"enable_debug_logger" mapstructure:"enable_debug_logger" json:"enableDebugLogger,omitempty" gorm:"column:enableDebugLogger" bson:"enableDebugLogger,omitempty" dynamodbav:"enableDebugLogger,omitempty" firestore:"enableDebugLogger,omitempty"`
	DisableMetaHeader     *bool           `yaml:"disable_meta_header" mapstructure:"disable_meta_header" json:"disableMetaHeader,omitempty" gorm:"column:disablemetaheader" bson:"disableMetaHeader,omitempty" dynamodbav:"disableMetaHeader,omitempty" firestore:"disableMetaHeader,omitempty"`
	Transport             TransportConfig `yaml:"transport" mapstructure:"transport" json:"transport,omitempty" gorm:"column:transport" bson:"transport,omitempty" dynamodbav:"transport,omitempty" firestore:"transport,omitempty"`
}

func GetConfig(conf Config, timeouts ...time.Duration) elasticsearch.Config {
	t := http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS11}}
	if conf.Transport.MaxIdleConnsPerHost != nil {
		t.MaxConnsPerHost = *conf.Transport.MaxIdleConnsPerHost
	} else {
		t.MaxConnsPerHost = 10
	}
	if conf.Transport.ResponseHeaderTimeout != nil {
		t.ResponseHeaderTimeout = time.Duration(*conf.Transport.ResponseHeaderTimeout) * time.Millisecond
	} else {
		t.ResponseHeaderTimeout = time.Minute
	}
	if len(timeouts) >= 1 {
		t.DialContext = (&net.Dialer{Timeout: timeouts[0]}).DialContext
	} else if conf.Transport.Timeout != nil {
		t.DialContext = (&net.Dialer{Timeout: time.Duration(*conf.Transport.Timeout) * time.Millisecond}).DialContext
	} else {
		t.DialContext = (&net.Dialer{Timeout: 4 * time.Second}).DialContext
	}
	c := elasticsearch.Config{
		Addresses: conf.Addresses,
		Transport: &t,
	}
	if conf.Username != nil {
		c.Username = *conf.Username
	}
	if conf.Password != nil {
		c.Password = *conf.Password
	}
	if conf.CloudID != nil {
		c.CloudID = *conf.CloudID
	}
	if conf.APIKey != nil {
		c.APIKey = *conf.APIKey
	}
	if conf.DisableRetry != nil {
		c.DisableRetry = *conf.DisableRetry
	}
	/*
		if conf.EnableRetryOnTimeout != nil {
			c.EnableRetryOnTimeout = *conf.EnableRetryOnTimeout
		}
	*/
	if conf.MaxRetries != nil {
		c.MaxRetries = *conf.MaxRetries
	}
	if conf.DiscoverNodesOnStart != nil {
		c.DiscoverNodesOnStart = *conf.DiscoverNodesOnStart
	}
	if conf.DiscoverNodesInterval != nil {
		c.DiscoverNodesInterval = time.Duration(*conf.DiscoverNodesInterval) * time.Millisecond
	}
	if conf.EnableMetrics != nil {
		c.EnableMetrics = *conf.EnableMetrics
	}
	if conf.EnableDebugLogger != nil {
		c.EnableDebugLogger = *conf.EnableDebugLogger
	}
	if conf.DisableMetaHeader != nil {
		c.DisableMetaHeader = *conf.DisableMetaHeader
	}
	return c
}
