package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type ElasticSearchUpdater struct {
	client    *elasticsearch.Client
	indexName string
}

func NewElasticSearchUpdater(client *elasticsearch.Client, indexName string) *ElasticSearchUpdater {
	return &ElasticSearchUpdater{client, indexName}
}

func (e *ElasticSearchUpdater) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, err := UpdateOne(ctx, e.client, e.indexName, modelType, model)
	return err
}
