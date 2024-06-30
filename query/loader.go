package query

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type Loader[T any] struct {
	Client  *elasticsearch.Client
	Index   string
	idIndex int
	idJson  string
}

func NewAdapter[T any](client *elasticsearch.Client, index string) *Loader[T] {
	return NewAdapterWithIdName[T](client, index, "")
}
func NewAdapterWithIdName[T any](client *elasticsearch.Client, index string, idFieldName string) *Loader[T] {
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
	return &Loader[T]{Client: client, Index: index, idIndex: idIndex, idJson: idJson}
}
func (a *Loader[T]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := make(map[string]interface{})
	err := es.Find(ctx, a.Client, []string{"users"}, query, &objs, a.idJson)
	return objs, err
}
func (a *Loader[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, err := es.FindOne(ctx, a.Client, a.Index, id, &obj, a.idJson)
	if !ok || err != nil {
		return nil, err
	}
	return &obj, nil
}
func (a *Loader[T]) Exist(ctx context.Context, id string) (bool, error) {
	return es.Exist(ctx, a.Client, a.Index, id)
}
