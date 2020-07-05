package elasticsearch

import (
	"context"
	"github.com/common-go/search"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type SearchResultBuilder interface {
	BuildSearchResult(ctx context.Context, db *elasticsearch.Client, searchModel interface{}, modelType reflect.Type, indexName string) (*search.SearchResult, error)
}
