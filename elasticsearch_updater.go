package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type ElasticsearchUpdater struct {
	client    *elasticsearch.Client
	indexName string
}

func NewElasticsearchUpdater(client *elasticsearch.Client, indexName string) *ElasticsearchUpdater {
	return &ElasticsearchUpdater{client, indexName}
}

func (e *ElasticsearchUpdater) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, err := UpdateOne(ctx, e.client, e.indexName, modelType, model)
	return err
}
