package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch/v7"
)

type SearchBuilder struct {
	Client     *elasticsearch.Client
	IndexName  string
	BuildQuery func(searchModel interface{}) map[string]interface{}
	GetSort    func(m interface{}) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
	ModelType  reflect.Type
}

func NewSearchBuilder(client *elasticsearch.Client, indexName string, modelType reflect.Type, buildQuery func(interface{}) map[string]interface{}, getSort func(m interface{}) string, options ...func(context.Context, interface{}) (interface{}, error)) *SearchBuilder {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &SearchBuilder{Client: client, IndexName: indexName, BuildQuery: buildQuery, GetSort: getSort, Map: mp, ModelType: modelType}
}
func (b *SearchBuilder) Search(ctx context.Context, sm interface{}, results interface{}, pageSize int64, skip int64) (int64, error) {
	query := b.BuildQuery(sm)
	s := b.GetSort(sm)
	sort := BuildSort(s)
	total, err := BuildSearchResult(ctx, b.Client, results, b.IndexName, query, sort, pageSize, skip, b.ModelType, b.Map)
	return total, err
}

func UpdateQuery(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	result["query"] = map[string]interface{}{
		"bool": map[string]interface{}{
			"must": make([]map[string]interface{}, 0),
		},
	}
	queryFields := make([]map[string]interface{}, 0)
	for key, value := range m {
		q := make(map[string]interface{})
		if reflect.ValueOf(value).Kind() == reflect.Map {
			q["range"] = make(map[string]interface{})
			q["range"].(map[string]interface{})[key] = make(map[string]interface{})
			for operator, val := range value.(map[string]interface{}) {
				q["range"].(map[string]interface{})[key].(map[string]interface{})[operator[1:]] = val
			}
		} else {
			q["prefix"] = make(map[string]interface{})
			q["prefix"].(map[string]interface{})[key] = value
		}
		queryFields = append(queryFields, q)
	}
	result["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"] = queryFields
	return result
}
