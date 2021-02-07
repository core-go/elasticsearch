package elasticsearch

import (
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

func NewGenericSearchService(db *elasticsearch.Client, indexName string, modelType reflect.Type, searchBuilder SearchResultBuilder, options ...string) (*GenericService, *SearchService) {
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	genericService := NewGenericService(db, indexName, modelType, versionField)
	searchService := NewSearchService(db, indexName, modelType, searchBuilder)
	return genericService, searchService
}
