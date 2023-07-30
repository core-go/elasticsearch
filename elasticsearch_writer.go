package elasticsearch

import (
	"context"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"reflect"
)

type ElasticSearchWriter struct {
	client    *elasticsearch.Client
	indexName string
	modelType reflect.Type
	idIndex   int
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewElasticSearchWriter(client *elasticsearch.Client, indexName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (*ElasticSearchWriter, error) {
	idIndex, _, _ := FindIdField(modelType)
	if idIndex < 0 {
		return nil, errors.New(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &ElasticSearchWriter{client: client, indexName: indexName, Map: mp}, nil
}

func (w *ElasticSearchWriter) Write(ctx context.Context, model interface{}) error {
	modelValue := reflect.ValueOf(model)
	id := modelValue.Field(w.idIndex).String()
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
