package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch"
	"log"
	"reflect"
)

type ViewService struct {
	client    *elasticsearch.Client
	indexName string
	modelType reflect.Type
	idName    string
	idIndex   int
}

func NewViewService(db *elasticsearch.Client, indexName string, modelType reflect.Type) *ViewService {
	idIndex, idName := FindIdField(modelType)
	if len(idName) == 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return &ViewService{db, indexName, modelType, idName, idIndex}
}

func (m *ViewService) Keys() []string {
	return []string{m.indexName}
}

func (m *ViewService) All(ctx context.Context) (interface{}, error) {
	query := BuildQuery(m.indexName, nil)
	return Find(ctx, m.client, []string{m.indexName}, query, m.modelType)
}

func (m *ViewService) Load(ctx context.Context, id interface{}) (interface{}, error) {
	sid := id.(string)
	return FindOneById(ctx, m.client, m.indexName, sid, m.modelType)
}

func (m *ViewService) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	sid := id.(string)
	return FindOneByIdAndDecode(ctx, m.client, m.indexName, sid, result)
}

func (m *ViewService) Exist(ctx context.Context, id interface{}) (bool, error) {
	sid := id.(string)
	return Exist(ctx, m.client, m.indexName, sid)
}
