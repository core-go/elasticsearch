package elasticsearch

import (
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

func NewGenericSearchService(db *elasticsearch.Client, indexName string, modelType reflect.Type, versionField string, searchBuilder SearchResultBuilder) (*GenericService, *SearchService) {
	genericService := NewGenericService(db, indexName, modelType, versionField)
	searchService := NewSearchService(db, indexName, modelType, searchBuilder)
	return genericService, searchService
}
