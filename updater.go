package elasticsearch

import (
	"context"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"reflect"
)

type Updater struct {
	client    *elasticsearch.Client
	indexName string
	modelType reflect.Type
	idIndex   int
	Map       func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewUpdater(client *elasticsearch.Client, indexName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (*Updater, error) {
	idIndex, _, _ := FindIdField(modelType)
	if idIndex < 0 {
		return nil, errors.New(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &Updater{client: client, indexName: indexName, modelType: modelType, idIndex: idIndex, Map: mp}, nil
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := UpdateOne(ctx, w.client, w.indexName, m2, w.modelType, w.idIndex)
		return er1
	}
	_, er2 := UpdateOne(ctx, w.client, w.indexName, model, w.modelType, w.idIndex)
	return er2
}
