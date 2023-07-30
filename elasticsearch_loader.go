package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8"
	"log"
	"reflect"
)

type ElasticSearchLoader struct {
	client     *elasticsearch.Client
	indexName  string
	modelType  reflect.Type
	jsonIdName string
	idIndex    int
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewElasticSearchLoader(client *elasticsearch.Client, indexName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *ElasticSearchLoader {
	idIndex, _, jsonIdName := FindIdField(modelType)
	if idIndex < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}
	return &ElasticSearchLoader{client: client, indexName: indexName, modelType: modelType, jsonIdName: jsonIdName, idIndex: idIndex, Map: mp}
}

func (m *ElasticSearchLoader) Id() string {
	return m.indexName
}

func (m *ElasticSearchLoader) All(ctx context.Context) (interface{}, error) {
	query := BuildQueryMap(m.indexName, nil)
	result, err := Find(ctx, m.client, []string{m.indexName}, query, m.modelType)
	if m.Map != nil && err == nil && result != nil {
		return MapModels(ctx, result, m.Map)
	}
	return result, err
}

func (m *ElasticSearchLoader) Load(ctx context.Context, id string) (interface{}, error) {
	r, er1 := FindOneById(ctx, m.client, m.indexName, id, m.modelType)
	if er1 != nil {
		return r, er1
	}
	if m.Map != nil {
		r2, er2 := m.Map(ctx, r)
		if er2 != nil {
			return r, er2
		}
		return r2, er2
	}
	return r, er1
}

func (m *ElasticSearchLoader) Get(ctx context.Context, id string, result interface{}) (bool, error) {
	ok, er0 := FindOneByIdAndDecode(ctx, m.client, m.indexName, id, result)
	if ok && er0 == nil && m.Map != nil {
		_, er2 := m.Map(ctx, result)
		if er2 != nil {
			return ok, er2
		}
	}
	return ok, er0
}

func (m *ElasticSearchLoader) LoadAndDecode(ctx context.Context, id string, result interface{}) (bool, error) {
	return m.Get(ctx, id, result)
}

func (m *ElasticSearchLoader) Exist(ctx context.Context, id string) (bool, error) {
	return Exist(ctx, m.client, m.indexName, id)
}
