package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

type ElasticSearchWriter struct {
	client    *elasticsearch.Client
	indexName string
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewElasticSearchWriter(client *elasticsearch.Client, indexName string, options ...func(context.Context, interface{}) (interface{}, error)) *ElasticSearchWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &ElasticSearchWriter{client: client, indexName: indexName, Map: mp}
}

func (w *ElasticSearchWriter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, _, id := FindValueByJson(modelType, "id")
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := UpsertOne(ctx, w.client, w.indexName, id, m2)
		return er1
	}
	_, er2 := UpsertOne(ctx, w.client, w.indexName, id, model)
	return er2
}
