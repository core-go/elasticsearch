package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

type ElasticSearchWriter struct {
	client    *elasticsearch.Client
	indexName string
}

func NewElasticSearchWriter(client *elasticsearch.Client, indexName string) *ElasticSearchWriter {
	return &ElasticSearchWriter{client, indexName}
}

func (e *ElasticSearchWriter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, _, id := FindValueByJson(modelType, "id")
	_, err := UpsertOne(ctx, e.client, e.indexName, id, model)
	return err
}
