package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type ElasticSearchInserter struct {
	client    *elasticsearch.Client
	indexName string
}

func NewElasticSearchInserter(client *elasticsearch.Client, indexName string) *ElasticSearchInserter {
	return &ElasticSearchInserter{client, indexName}
}

func (e *ElasticSearchInserter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, err := InsertOne(ctx, e.client, e.indexName, modelType, model)
	return err
}
