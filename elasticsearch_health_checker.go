package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"time"
)

type ElasticsearchHealthChecker struct {
	client  *elasticsearch.Client
	name    string
	timeout time.Duration
}

func NewElasticsearchHealthChecker(client *elasticsearch.Client, name string, timeout time.Duration) *ElasticsearchHealthChecker {
	return &ElasticsearchHealthChecker{client, name, timeout}
}

func NewDefaultElasticsearchHealthChecker(client *elasticsearch.Client) *ElasticsearchHealthChecker {
	return &ElasticsearchHealthChecker{client, "Elasticsearch", 5 * time.Second}
}

func (e *ElasticsearchHealthChecker) Name() string {
	return e.name
}

func (e *ElasticsearchHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{})
	_, err := e.client.Ping()
	if err != nil {
		return nil, err
	}
	res["status"] = "success"
	return res, nil
}

func (e *ElasticsearchHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	data["error"] = err.Error()
	return data
}
