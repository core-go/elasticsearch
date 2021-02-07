package elasticsearch

import (
	"context"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"reflect"
)

type GenericService struct {
	*ViewService
	maps         map[string]string
	versionField string
	versionIndex int
}

func NewGenericService(db *elasticsearch.Client, indexName string, modelType reflect.Type, options ...string) *GenericService {
	defaultViewService := NewViewService(db, indexName, modelType)
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	if len(versionField) > 0 {
		index, _ := FindFieldByName(modelType, versionField)
		if index >= 0 {
			return &GenericService{ViewService: defaultViewService, maps: MakeMapJson(modelType), versionField: versionField, versionIndex: index}
		}
	}
	return &GenericService{ViewService: defaultViewService, maps: MakeMapJson(modelType), versionField: "", versionIndex: -1}
}

func (m *GenericService) Insert(ctx context.Context, model interface{}) (int64, error) {
	return InsertOne(ctx, m.client, m.indexName, m.modelType, model)
}

func (m *GenericService) Update(ctx context.Context, model interface{}) (int64, error) {
	return UpdateOne(ctx, m.client, m.indexName, m.modelType, model)
}
func (m *GenericService) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	return PatchOne(ctx, m.client, m.indexName, MapToDBObject(model, m.maps))
}

func (m *GenericService) Delete(ctx context.Context, id interface{}) (int64, error) {
	sid := id.(string)
	return DeleteOne(ctx, m.client, m.indexName, sid)
}

func (m *GenericService) Save(ctx context.Context, model interface{}) (int64, error) {
	idIndex, _ := FindIdField(m.modelType)
	if idIndex < 0 {
		return 0, fmt.Errorf("missing document ID in the object")
	}
	modelValue := reflect.ValueOf(model)
	id := modelValue.Field(idIndex).String()
	return UpsertOne(ctx, m.client, m.indexName, id, model)
}
