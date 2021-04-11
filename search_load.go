package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

func NewSearchLoader(client *elasticsearch.Client, indexName string, modelType reflect.Type, search func(context.Context, interface{}) (interface{}, int64, error)) (*Searcher, *Loader) {
	searcher := NewSearcher(search)
	loader := NewLoader(client, indexName, modelType)
	return searcher, loader
}

func NewDefaultSearchLoader(client *elasticsearch.Client, indexName string, modelType reflect.Type, options...func(m interface{}) (string, int64, int64, int64, error)) (*Searcher, *Loader) {
	searcher := NewDefaultSearcher(client, indexName, modelType, options...)
	loader := NewLoader(client, indexName, modelType)
	return searcher, loader
}
