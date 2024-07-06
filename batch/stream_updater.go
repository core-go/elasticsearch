package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type StreamUpdater[T any] struct {
	client    *elasticsearch.Client
	index     string
	idx       int
	Map       func(T)
	isPointer bool
	FieldMap  []es.FieldMap
	batch     []Model
	batchSize int
}

func NewStreamUpdater[T any](client *elasticsearch.Client, index string, batchSize int, opts ...func(T)) *StreamUpdater[T] {
	return NewStreamUpdaterWithIdName[T](client, index, batchSize, "", opts...)
}
func NewStreamUpdaterWithIdName[T any](client *elasticsearch.Client, index string, batchSize int, idFieldName string, opts ...func(T)) *StreamUpdater[T] {
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
	initModel := reflect.New(modelType).Interface()
	vo := reflect.Indirect(reflect.ValueOf(initModel))
	id := vo.Field(idx).Interface()
	_, ok := id.(string)
	if !ok {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	var mp func(T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	return &StreamUpdater[T]{client: client, index: index, idx: idx, FieldMap: es.BuildMap(modelType), batch: make([]Model, 0), batchSize: batchSize, Map: mp, isPointer: isPointer}
}
func (w *StreamUpdater[T]) Write(ctx context.Context, obj T) error {
	if w.Map != nil {
		w.Map(obj)
	}
	vo := reflect.ValueOf(obj)
	if w.isPointer {
		vo = reflect.Indirect(vo)
	}
	id := vo.Field(w.idx).Interface().(string)
	body := es.BuildBody(obj, w.FieldMap)
	model := Model{Id: id, Body: body}
	data, err := json.Marshal(model.Body)
	if err != nil {
		return err
	}
	model.Data = string(data)
	w.batch = append(w.batch, model)
	if len(w.batch) >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}
func (w *StreamUpdater[T]) Flush(ctx context.Context) error {
	_, err := UpdateBatch(ctx, w.client, w.index, w.batch)
	w.batch = make([]Model, 0)
	return err
}
