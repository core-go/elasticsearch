package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type SearchResultBuilder interface {
	Search(ctx context.Context, db *elasticsearch.Client, searchModel interface{}, modelType reflect.Type, indexName string) (interface{}, int64, error)
}
