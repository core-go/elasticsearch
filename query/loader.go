package query

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type Loader[T any] struct {
	Client      *elasticsearch.Client
	Index       string
	idIndex     int
	idJson      string
	versionJson string
	Map         func(*T)
}

func NewAdapter[T any](client *elasticsearch.Client, index string, opts ...func(*T)) *Loader[T] {
	return NewAdapterWithIdName[T](client, index, "", "", opts...)
}
func NewAdapterWithVersion[T any](client *elasticsearch.Client, index string, versionJson string, opts ...func(*T)) *Loader[T] {
	return NewAdapterWithIdName[T](client, index, versionJson, "", opts...)
}
func NewAdapterWithIdName[T any](client *elasticsearch.Client, index string, versionJson string, idFieldName string, opts ...func(*T)) *Loader[T] {
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
	var mp func(*T)
	if len(opts) > 0 && opts[0] != nil {
		mp = opts[0]
	}
	return &Loader[T]{Client: client, Index: index, idIndex: idIndex, idJson: idJson, versionJson: versionJson, Map: mp}
}
func (a *Loader[T]) All(ctx context.Context) ([]T, error) {
	var objs []T
	query := make(map[string]interface{})
	err := es.FindWithVersion(ctx, a.Client, []string{a.Index}, query, &objs, a.idJson, a.versionJson)
	if a.Map != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			a.Map(&objs[i])
		}
	}
	return objs, err
}
func (a *Loader[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, err := es.FindOneWithVersion(ctx, a.Client, a.Index, id, &obj, a.idJson, a.versionJson)
	if !ok || err != nil {
		return nil, err
	}
	if a.Map != nil {
		a.Map(&obj)
	}
	return &obj, nil
}
func (a *Loader[T]) Exist(ctx context.Context, id string) (bool, error) {
	return es.Exist(ctx, a.Client, a.Index, id)
}
