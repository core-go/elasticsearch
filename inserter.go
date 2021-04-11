package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

type Inserter struct {
	client    *elasticsearch.Client
	indexName string
}

func NewInserter(client *elasticsearch.Client, indexName string) *Inserter {
	return &Inserter{client, indexName}
}

func (e *Inserter) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, err := InsertOne(ctx, e.client, e.indexName, modelType, model)
	return err
}
