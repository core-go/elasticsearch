package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type ElasticsearchInserter struct {
	client    *elasticsearch.Client
	indexName string
}

func NewElasticsearchInserter(client *elasticsearch.Client, indexName string) *ElasticsearchInserter {
	return &ElasticsearchInserter{client, indexName}
}

func (e *ElasticsearchInserter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, err := InsertOne(ctx, e.client, e.indexName, modelType, model)
	return err
}
