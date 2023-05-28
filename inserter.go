package elasticsearch

import (
	"context"
	"errors"
	es "github.com/elastic/go-elasticsearch/v7"
	"reflect"
)

type Inserter struct {
	client    *es.Client
	indexName string
	modelType reflect.Type
	idIndex   int
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewInserter(client *es.Client, indexName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (*Inserter, error) {
	idIndex, _, _ := FindIdField(modelType)
	if idIndex < 0 {
		return nil, errors.New(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Inserter{client: client, indexName: indexName, Map: mp}, nil
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := InsertOne(ctx, w.client, w.indexName, m2, modelType)
		return er1
	}
	_, er2 := InsertOne(ctx, w.client, w.indexName, model, modelType)
	return er2
}
