package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8"
	"reflect"
)

func NewDefaultSearchLoader(client *elasticsearch.Client, indexName string, modelType reflect.Type, search func(context.Context, interface{}, interface{}, int64, int64) (int64, error), options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader) {
	searcher := NewSearcher(search)
	loader := NewLoader(client, indexName, modelType, options...)
	return searcher, loader
}

func NewSearchLoader(client *elasticsearch.Client, indexName string, modelType reflect.Type, buildQuery func(interface{}) map[string]interface{}, getSort func(m interface{}) string, options ...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader) {
	searcher := NewSearcherWithQuery(client, indexName, modelType, buildQuery, getSort, options...)
	loader := NewLoader(client, indexName, modelType, options...)
	return searcher, loader
}
