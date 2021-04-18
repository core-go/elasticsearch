package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
)

type SearchBuilder struct {
	Client     *elasticsearch.Client
	IndexName  string
	BuildQuery func(searchModel interface{}) map[string]interface{}
	GetSort    func(m interface{}) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSearchBuilder(client *elasticsearch.Client, indexName string, buildQuery func(interface{}) map[string]interface{}, getSort func(m interface{}) string, options ...func(context.Context, interface{}) (interface{}, error)) *SearchBuilder {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &SearchBuilder{Client: client, IndexName: indexName, BuildQuery: buildQuery, GetSort: getSort, Map: mp}
}
func (b *SearchBuilder) Search(ctx context.Context, sm interface{}, results interface{}, pageIndex int64, pageSize int64, options ...int64) (int64, error) {
	query := b.BuildQuery(sm)
	s := b.GetSort(sm)
	var sort []string
	sort = BuildSort(s)
	var firstPageSize int64
	if len(options) > 0 && options[0] > 0 {
		firstPageSize = options[0]
	} else {
		firstPageSize = 0
	}
	return BuildSearchResult(ctx, b.Client, results, b.IndexName, query, sort, pageIndex, pageSize, firstPageSize, b.Map)
}
