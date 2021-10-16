package elasticsearch

import (
	"context"
	"fmt"
	es "github.com/elastic/go-elasticsearch/v7"
	"reflect"
)

type Repository interface {
	Get(ctx context.Context, id string, result interface{}) (bool, error)
	Exist(ctx context.Context, id string) (bool, error)
	Insert(ctx context.Context, model interface{}) (int64, error)
	Update(ctx context.Context, model interface{}) (int64, error)
	Patch(ctx context.Context, model map[string]interface{}) (int64, error)
	Delete(ctx context.Context, id string) (int64, error)
}

type GenericWriter struct {
	*ElasticSearchLoader
	maps         map[string]string
	versionField string
	versionIndex int
	Mapper       Mapper
}

func NewGenericWriter(client *es.Client, indexName string, modelType reflect.Type, options ...string) *GenericWriter {
	return NewGenericWriterWithMapper(client, indexName, modelType, nil, options...)
}
func NewGenericWriterWithMapper(client *es.Client, indexName string, modelType reflect.Type, mapper Mapper, options ...string) *GenericWriter {
	var loader *ElasticSearchLoader
	if mapper != nil {
		loader = NewElasticSearchLoader(client, indexName, modelType, mapper.DbToModel)
	} else {
		loader = NewElasticSearchLoader(client, indexName, modelType)
	}
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	if len(versionField) > 0 {
		index, _ := FindFieldByName(modelType, versionField)
		if index >= 0 {
			return &GenericWriter{ElasticSearchLoader: loader, maps: MakeMapJson(modelType), versionField: versionField, versionIndex: index}
		}
	}
	return &GenericWriter{ElasticSearchLoader: loader, maps: MakeMapJson(modelType), Mapper: mapper, versionField: "", versionIndex: -1}
}

func (m *GenericWriter) Insert(ctx context.Context, model interface{}) (int64, error) {
	return InsertOne(ctx, m.client, m.indexName, m.modelType, model)
}

func (m *GenericWriter) Update(ctx context.Context, model interface{}) (int64, error) {
	return UpdateOne(ctx, m.client, m.indexName, m.modelType, model)
}
func (m *GenericWriter) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	return PatchOne(ctx, m.client, m.indexName, MapToDBObject(model, m.maps))
}

func (m *GenericWriter) Delete(ctx context.Context, id interface{}) (int64, error) {
	sid := id.(string)
	return DeleteOne(ctx, m.client, m.indexName, sid)
}

func (m *GenericWriter) Save(ctx context.Context, model interface{}) (int64, error) {
	idIndex, _, _ := FindIdField(m.modelType)
	if idIndex < 0 {
		return 0, fmt.Errorf("missing document ID in the object")
	}
	modelValue := reflect.ValueOf(model)
	id := modelValue.Field(idIndex).String()
	return UpsertOne(ctx, m.client, m.indexName, id, model)
}
