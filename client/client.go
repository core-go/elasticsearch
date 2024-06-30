package client

import (
	"github.com/elastic/go-elasticsearch/v8"
	"time"
)

func NewClient(config Config, timeouts ...time.Duration) (*elasticsearch.Client, error) {
	c := GetConfig(config, timeouts...)
	return elasticsearch.NewClient(c)
}
