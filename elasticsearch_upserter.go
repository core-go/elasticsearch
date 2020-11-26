package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type ElasticsearchUpserter struct {
	client    *elasticsearch.Client
	indexName string
}

func NewElasticsearchUpserter(client *elasticsearch.Client, indexName string) *ElasticsearchUpserter {
	return &ElasticsearchUpserter{client, indexName}
}

func (e *ElasticsearchUpserter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, _, id := FindValueByJson(modelType, "id")
	_, err := UpsertOne(ctx, e.client, e.indexName, id, model)
	return err
}
