package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
)

type Searcher struct {
	search func(ctx context.Context, searchModel interface{}, results interface{}, pageIndex int64, pageSize int64, options ...int64) (int64, error)
}

func NewSearcher(search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, pageIndex int64, pageSize int64, options ...int64) (int64, error) {
	return s.search(ctx, m, results, pageIndex, pageSize, options...)
}

func NewSearcherWithQuery(client *elasticsearch.Client, indexName string, buildQuery func(interface{}) map[string]interface{}, getSort func(m interface{}) string, options ...func(context.Context, interface{}) (interface{}, error)) *Searcher {
	builder := NewSearchBuilder(client, indexName, buildQuery, getSort, options...)
	return NewSearcher(builder.Search)
}
