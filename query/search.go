package query

import (
	"context"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type Query[T any, F any] struct {
	*Loader[T]
	BuildQuery func(F) map[string]interface{}
	GetSort    func(interface{}) string
	ModelType  reflect.Type
}

func NewQuery[T any, F any](client *elasticsearch.Client, index string, buildQuery func(F) map[string]interface{}, getSort func(interface{}) string, opts ...func(*T)) *Query[T, F] {
	return NewQueryWithIdName[T, F](client, index, buildQuery, getSort, "", "", opts...)
}
func NewQueryWithVersion[T any, F any](client *elasticsearch.Client, index string, buildQuery func(F) map[string]interface{}, getSort func(interface{}) string, versionJson string, opts ...func(*T)) *Query[T, F] {
	return NewQueryWithIdName[T, F](client, index, buildQuery, getSort, "", versionJson, opts...)
}
func NewQueryWithIdName[T any, F any](client *elasticsearch.Client, index string, buildQuery func(F) map[string]interface{}, getSort func(interface{}) string, idName string, versionJson string, opts ...func(*T)) *Query[T, F] {
	adapter := NewAdapterWithIdName[T](client, index, idName, versionJson, opts...)
	var t T
	modelType := reflect.TypeOf(t)
	return &Query[T, F]{adapter, buildQuery, getSort, modelType}
}

func (b *Query[T, F]) Search(ctx context.Context, filter F, limit int64, offset int64) ([]T, int64, error) {
	query := b.BuildQuery(filter)
	s := b.GetSort(filter)
	sort := es.BuildSort(s, b.ModelType)
	var objs []T
	total, err := es.BuildSearchResult(ctx, b.Client, []string{b.Index}, &objs, b.idJson, query, sort, limit, offset, b.versionJson)
	if b.Map != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			b.Map(&objs[i])
		}
	}
	return objs, total, err
}
