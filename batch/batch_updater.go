package batch

import (
	"context"
	"fmt"
	"reflect"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
)

type BatchUpdater[T any] struct {
	client   *elasticsearch.Client
	index    string
	idx      int
	Map      func(*T)
	FieldMap []es.FieldMap
	retryAll bool
}

func NewBatchUpdater[T any](client *elasticsearch.Client, index string, retryAll bool, opts ...func(*T)) *BatchUpdater[T] {
	return NewBatchUpdaterWithIdName[T](client, index, retryAll, "", opts...)
}
func NewBatchUpdaterWithIdName[T any](client *elasticsearch.Client, index string, retryAll bool, idFieldName string, opts ...func(*T)) *BatchUpdater[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
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
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	return &BatchUpdater[T]{client: client, index: index, idx: idx, FieldMap: es.BuildMap(modelType), Map: mp, retryAll: retryAll}
}
func (w *BatchUpdater[T]) Write(ctx context.Context, objs []T) ([]int, error) {
	le := len(objs)
	if le <= 0 {
		return nil, nil
	}
	if w.Map != nil {
		for i := 0; i < le; i++ {
			w.Map(&objs[i])
		}
	}
	models, err := BuildModels[T](objs, w.idx, w.FieldMap)
	if err != nil {
		if !w.retryAll {
			return nil, err
		} else {
			failIndices := make([]int, 0)
			for i := 0; i < le; i++ {
				failIndices = append(failIndices, i)
			}
			return failIndices, err
		}

	}
	return UpdateBatch(ctx, w.client, w.index, models)
}
