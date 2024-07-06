package adapter

import (
	"context"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type SearchAdapter[T any, F any] struct {
	*Adapter[T]
	BuildQuery func(F) map[string]interface{}
	GetSort    func(interface{}) string
	ModelType  reflect.Type
	Map        func(*T)
}

func NewSearchAdapter[T any, F any](client *elasticsearch.Client, index string, buildQuery func(F) map[string]interface{}, getSort func(interface{}) string, opts ...func(*T)) *SearchAdapter[T, F] {
	return NewSearchAdapterWithIdName[T, F](client, index, buildQuery, getSort, "", opts...)
}
func NewSearchAdapterWithIdName[T any, F any](client *elasticsearch.Client, index string, buildQuery func(F) map[string]interface{}, getSort func(interface{}) string, idName string, opts ...func(*T)) *SearchAdapter[T, F] {
	adapter := NewAdapterWithIdName[T](client, index, idName)
	var t T
	modelType := reflect.TypeOf(t)
	var mp func(*T)
	if len(opts) > 0 && opts[0] != nil {
		mp = opts[0]
	}
	return &SearchAdapter[T, F]{adapter, buildQuery, getSort, modelType, mp}
}

func (b *SearchAdapter[T, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
	query := b.BuildQuery(filter)
	s := b.GetSort(filter)
	sort := es.BuildSort(s, b.ModelType)
	var objs []T
	total, err := es.BuildSearchResult(ctx, b.Client, []string{b.Index}, &objs, b.idJson, query, sort, limit, offset, "")
	if b.Map != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			b.Map(&objs[i])
		}
	}
	return objs, total, err
}
