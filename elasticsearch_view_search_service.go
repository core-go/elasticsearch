package elasticsearch

import (
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

func NewViewSearchService(db *elasticsearch.Client, modelType reflect.Type, indexName string, searchBuilder SearchResultBuilder) (*ViewService, *SearchService) {
	viewService := NewViewService(db, indexName, modelType)
	searchService := NewSearchService(db, indexName, modelType, searchBuilder)
	return viewService, searchService
}
