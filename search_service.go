package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type SearchService struct {
	client        *elasticsearch.Client
	indexName     string
	modelType     reflect.Type
	searchBuilder SearchResultBuilder
}

func NewSearchService(db *elasticsearch.Client, indexName string, modelType reflect.Type, searchBuilder SearchResultBuilder) *SearchService {
	return &SearchService{db, indexName, modelType, searchBuilder}
}

func (s *SearchService) Search(ctx context.Context, m interface{}) (interface{}, int64, error) {
	return s.searchBuilder.BuildSearchResult(ctx, s.client, m, s.modelType, s.indexName)
}
