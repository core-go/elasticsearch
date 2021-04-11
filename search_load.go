package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

func NewSearchLoader(db *elasticsearch.Client, modelType reflect.Type, indexName string, search func(context.Context, interface{}) (interface{}, int64, error)) (*Searcher, *Loader) {
	searcher := NewSearchService(search)
	loader := NewViewService(db, indexName, modelType)
	return searcher, loader
}
