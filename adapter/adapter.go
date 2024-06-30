package adapter

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type Adapter[T any] struct {
	Client  *elasticsearch.Client
	Index   string
	idIndex int
	idJson  string
	Map     []es.FieldMap
}

func NewAdapter[T any](client *elasticsearch.Client, index string) *Adapter[T] {
	return NewAdapterWithIdName[T](client, index, "")
}
func NewAdapterWithIdName[T any](client *elasticsearch.Client, index string, idFieldName string) *Adapter[T] {
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
	vo := reflect.Indirect(reflect.ValueOf(t))
	id := vo.Field(idIndex).Interface()
	_, ok := id.(string)
	if !ok {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idIndex).Name, modelType.Name()))
	}
	return &Adapter[T]{Client: client, Index: index, idIndex: idIndex, idJson: idJson, Map: es.BuildMap(modelType)}
}
func (a *Adapter[T]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := make(map[string]interface{})
	err := es.Find(ctx, a.Client, []string{"users"}, query, &objs, a.idJson)
	return objs, err
}
func (a *Adapter[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, err := es.FindOne(ctx, a.Client, a.Index, id, &obj, a.idJson)
	if !ok || err != nil {
		return nil, err
	}
	return &obj, nil
}
func (a *Adapter[T]) Exist(ctx context.Context, id string) (bool, error) {
	return es.Exist(ctx, a.Client, a.Index, id)
}
func (a *Adapter[T]) Create(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	body := es.BuildBody(model, a.Map)
	if len(id) == 0 {
		return es.Create(ctx, a.Client, a.Index, body, nil)
	} else {
		return es.Create(ctx, a.Client, a.Index, body, &id)
	}
}
func (a *Adapter[T]) Update(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	body := es.BuildBody(model, a.Map)
	return es.Update(ctx, a.Client, a.Index, body, id)
}
func (a *Adapter[T]) Patch(ctx context.Context, data map[string]interface{}) (int64, error) {
	return es.Patch(ctx, a.Client, a.Index, data, a.idJson)
}
func (a *Adapter[T]) Save(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	if len(id) == 0 {
		return a.Create(ctx, model)
	}
	body := es.BuildBody(model, a.Map)
	return es.Save(ctx, a.Client, a.Index, body, id)
}
func (a *Adapter[T]) Delete(ctx context.Context, id string) (int64, error) {
	return es.Delete(ctx, a.Client, a.Index, id)
}
