package writer

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type Creator[T any] struct {
	client    *elasticsearch.Client
	index     string
	idx       int
	Map       func(T)
	isPointer bool
	FieldMap  []es.FieldMap
}

func NewCreator[T any](client *elasticsearch.Client, index string, opts ...func(T)) *Creator[T] {
	return NewCreatorWithIdName[T](client, index, "", opts...)
}
func NewCreatorWithIdName[T any](client *elasticsearch.Client, index string, idFieldName string, opts ...func(T)) *Creator[T] {
	var t T
	modelType := reflect.TypeOf(t)
	isPointer := false
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
		isPointer = true
	}
	var idx int
	if len(idFieldName) == 0 {
		idx, _, _ = es.FindIdField(modelType)
		if idx < 0 {
			panic("Require Id field of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _ = es.FindFieldByName(modelType, idFieldName)
		if idx < 0 {
			panic(fmt.Sprintf("%s struct requires id field which id name is '%s'", modelType.Name(), idFieldName))
		}
	}
	idField := modelType.Field(idx)
	if idField.Type.String() != "string" {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	var mp func(T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	return &Creator[T]{client: client, index: index, idx: idx, Map: mp, isPointer: isPointer}
}
func (w *Creator[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	vo := reflect.ValueOf(model)
	if w.isPointer {
		vo = reflect.Indirect(vo)
	}
	id := vo.Field(w.idx).Interface().(string)
	body := es.BuildBody(model, w.FieldMap)
	_, err := es.Create(ctx, w.client, w.index, body, id)
	return err
}
