package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

type Updater struct {
	client    *elasticsearch.Client
	indexName string
}

func NewUpdater(client *elasticsearch.Client, indexName string) *Updater {
	return &Updater{client, indexName}
}

func (e *Updater) Write(ctx context.Context, model interface{}) error {
	modelType := reflect.TypeOf(model)
	_, err := UpdateOne(ctx, e.client, e.indexName, modelType, model)
	return err
}
