package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

func NewSearchWriter(db *elasticsearch.Client, indexName string, modelType reflect.Type, search func(context.Context, interface{}) (interface{}, int64, error), options ...string) (*GenericService, *Searcher) {
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	genericService := NewGenericService(db, indexName, modelType, versionField)
	searchService := NewSearchService(search)
	return genericService, searchService
}
