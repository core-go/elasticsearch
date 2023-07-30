package elasticsearch

import (
	"context"
	es "github.com/elastic/go-elasticsearch/v8"
	"reflect"
)

type Mapper interface {
	DbToModel(ctx context.Context, model interface{}) (interface{}, error)
	ModelToDb(ctx context.Context, model interface{}) (interface{}, error)
}

type Writer struct {
	*Loader
	maps         map[string]string
	versionField string
	versionIndex int
	Mapper       Mapper
}

func NewWriter(client *es.Client, indexName string, modelType reflect.Type, options ...string) *Writer {
	return NewWriterWithMapper(client, indexName, modelType, nil, options...)
}
func NewWriterWithMapper(client *es.Client, indexName string, modelType reflect.Type, mapper Mapper, options ...string) *Writer {
	var loader *Loader
	if mapper != nil {
		loader = NewLoader(client, indexName, modelType, mapper.DbToModel)
	} else {
		loader = NewLoader(client, indexName, modelType)
	}
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	if len(versionField) > 0 {
		index, _ := FindFieldByName(modelType, versionField)
		if index >= 0 {
			return &Writer{Loader: loader, maps: MakeMapJson(modelType), versionField: versionField, versionIndex: index}
		}
	}
	return &Writer{Loader: loader, maps: MakeMapJson(modelType), Mapper: mapper, versionField: "", versionIndex: -1}
}

func (m *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	return InsertOne(ctx, m.client, m.indexName, model, m.idIndex)
}

func (m *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	return UpdateOne(ctx, m.client, m.indexName, model, m.idIndex)
}
func (m *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	return PatchOne(ctx, m.client, m.indexName, m.jsonIdName, MapToDBObject(model, m.maps))
}
func (m *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	sid := id.(string)
	return DeleteOne(ctx, m.client, m.indexName, sid)
}
func (m *Writer) Save(ctx context.Context, model interface{}) (int64, error) {
	modelValue := reflect.ValueOf(model)
	id := modelValue.Field(m.idIndex).String()
	return UpsertOne(ctx, m.client, m.indexName, id, model)
}
