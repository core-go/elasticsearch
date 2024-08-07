package adapter

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type Dao[T any] struct {
	Client       *elasticsearch.Client
	Index        string
	idIndex      int
	idJson       string
	versionIndex int
	versionJson  string
	Map          []es.FieldMap
}

func NewDao[T any](client *elasticsearch.Client, index string) *Dao[T] {
	return NewDaoWithIdName[T](client, index, "", "")
}
func NewDaoWithVersion[T any](client *elasticsearch.Client, index string, versionName string) *Dao[T] {
	return NewDaoWithIdName[T](client, index, "", versionName)
}
func NewDaoWithIdName[T any](client *elasticsearch.Client, index string, idFieldName string, versionName string) *Dao[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	var idIndex int
	var idJson string
	if len(idFieldName) == 0 {
		idIndex, _, idJson = es.FindIdField(modelType)
		if idIndex < 0 {
			panic(fmt.Sprintf("%s struct requires id field which has bson tag '_id'", modelType.Name()))
		}
	} else {
		idIndex, idJson = es.FindFieldByName(modelType, idFieldName)
		if idIndex < 0 {
			panic(fmt.Sprintf("%s struct requires id field which id name is '%s'", modelType.Name(), idFieldName))
		}
	}
	idField := modelType.Field(idIndex)
	if idField.Type.String() != "string" {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idIndex).Name, modelType.Name()))
	}
	versionIndex, versionJson := es.FindFieldByName(modelType, versionName)
	if versionIndex < 0 {
		versionJson = ""
	}
	return &Dao[T]{Client: client, Index: index, idIndex: idIndex, idJson: idJson, Map: es.BuildMap(modelType), versionIndex: versionIndex, versionJson: versionJson}
}
func (a *Dao[T]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := make(map[string]interface{})
	err := es.Find(ctx, a.Client, []string{"users"}, query, &objs, a.idJson)
	return objs, err
}
func (a *Dao[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, err := es.FindOneWithVersion(ctx, a.Client, a.Index, id, &obj, a.idJson, a.versionJson)
	if !ok || err != nil {
		return nil, err
	}
	return &obj, nil
}
func (a *Dao[T]) Exist(ctx context.Context, id string) (bool, error) {
	return es.Exist(ctx, a.Client, a.Index, id)
}
func (a *Dao[T]) Create(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	body := es.BuildBody(model, a.Map)
	return es.Create(ctx, a.Client, a.Index, body, id)
}
func (a *Dao[T]) Update(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	body := es.BuildBody(model, a.Map)
	return es.Update(ctx, a.Client, a.Index, body, id)
}
func (a *Dao[T]) Patch(ctx context.Context, data map[string]interface{}) (int64, error) {
	return es.Patch(ctx, a.Client, a.Index, data, a.idJson)
}
func (a *Dao[T]) Save(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	if len(id) == 0 {
		return a.Create(ctx, model)
	}
	body := es.BuildBody(model, a.Map)
	return es.Save(ctx, a.Client, a.Index, body, id)
}
func (a *Dao[T]) Delete(ctx context.Context, id string) (int64, error) {
	return es.Delete(ctx, a.Client, a.Index, id)
}
