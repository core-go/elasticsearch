package batch

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"reflect"
	"strings"
)

type BatchInserter struct {
	Es        *elasticsearch.Client
	IndexName string
	ModelType reflect.Type
}

func NewBatchInserter(es *elasticsearch.Client, indexName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchInserter {
	return &BatchInserter{Es: es, IndexName: indexName, ModelType: modelType}
}

func (w *BatchInserter) Write(ctx context.Context, model interface{}) ([]int, []int, error) {
	value := reflect.Indirect(reflect.ValueOf(model))
	var failureIndex, successIndices, failureIndices []int
	if value.Kind() == reflect.Slice && value.Len() > 0 {
		bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:  w.IndexName,
			Client: w.Es,
		})
		if err != nil {
			return successIndices, failureIndices, err
		}
		listIds := FindListIdField(w.ModelType, model)
		var successIds, failIds []interface{}
		for i := 0; i < value.Len(); i++ {
			sliceValue := value.Index(i).Interface()
			if idIndex, _, _ := FindIdField(w.ModelType); idIndex >= 0 {
				modelValue := reflect.Indirect(reflect.ValueOf(sliceValue))
				idValue := modelValue.Field(idIndex).String()
				if idValue != "" {
					body := BuildQueryWithoutIdFromObject(sliceValue)
					jsonBody, err := json.Marshal(body)
					if err != nil {
						return successIndices, failureIndices, err
					}
					er1 := bi.Add(context.Background(), esutil.BulkIndexerItem{
						Action:     "create",
						DocumentID: idValue,
						Body:       strings.NewReader(string(jsonBody)), // esutil.NewJSONReader(body),
						OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
							successIds = append(successIds, res.DocumentID)
						},
						OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
							failIds = append(failIds, res.DocumentID)
						},
					})
					if er1 != nil {
						failureIndex = append(failureIndex, int(i))
					}
				} else {
					failureIndex = append(failureIndex, int(i))
				}
			} else {
				failureIndex = append(failureIndex, int(i))
			}
		}
		if er2 := bi.Close(context.Background()); er2 != nil {
			return successIndices, failureIndices, er2
		}
		successIndices, failureIndices = BuildIndicesResult(listIds, successIds, failIds)
		failureIndices = append(failureIndices, failureIndex...)
		return successIndices, failureIndices, nil
	}
	return successIndices, failureIndices, errors.New("invalid input")
}
func FindIdField(modelType reflect.Type) (int, string, string) {
	return FindBsonField(modelType, "_id")
}
func FindBsonField(modelType reflect.Type, bsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		bsonTag := field.Tag.Get("bson")
		tags := strings.Split(bsonTag, ",")
		json := field.Name
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			json = strings.Split(tag1, ",")[0]
		}
		for _, tag := range tags {
			if strings.TrimSpace(tag) == bsonName {
				return i, field.Name, json
			}
		}
	}
	return -1, "", ""
}

func BuildQueryWithoutIdFromObject(object interface{}) map[string]interface{} {
	valueOf := reflect.Indirect(reflect.ValueOf(object))
	idIndex, _, _ := FindIdField(valueOf.Type())
	result := map[string]interface{}{}
	for i := 0; i < valueOf.NumField(); i++ {
		if i != idIndex {
			_, jsonName := FindFieldByIndex(valueOf.Type(), i)
			result[jsonName] = valueOf.Field(i).Interface()
		}
	}
	return result
}
func FindFieldByIndex(modelType reflect.Type, fieldIndex int) (fieldName, jsonTagName string) {
	if fieldIndex < modelType.NumField() {
		field := modelType.Field(fieldIndex)
		jsonTagName := ""
		if jsonTag, ok := field.Tag.Lookup("json"); ok {
			jsonTagName = strings.Split(jsonTag, ",")[0]
		}
		return field.Name, jsonTagName
	}
	return "", ""
}

func BuildIndicesResult(listIds, successIds, failIds []interface{}) (successIndices, failureIndices []int) {
	if len(listIds) > 0 {
		for _, idValue := range listIds {
			for index, id := range successIds {
				if id == idValue {
					successIndices = append(successIndices, int(index))
				}
			}
			for index, id := range failIds {
				if id == idValue {
					failureIndices = append(failureIndices, int(index))
				}
			}
		}
	}
	return
}

func InsertMany(ctx context.Context, es *elasticsearch.Client, indexName string, modelType reflect.Type, model interface{}) ([]int, []int, error) {
	value := reflect.Indirect(reflect.ValueOf(model))
	var failureIndex, successIndices, failureIndices []int
	if value.Kind() == reflect.Slice && value.Len() > 0 {
		bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:  indexName,
			Client: es,
		})
		if err != nil {
			return successIndices, failureIndices, err
		}
		listIds := FindListIdField(modelType, model)
		var successIds, failIds []interface{}
		for i := 0; i < value.Len(); i++ {
			sliceValue := value.Index(i).Interface()
			if idIndex, _, _ := FindIdField(modelType); idIndex >= 0 {
				modelValue := reflect.Indirect(reflect.ValueOf(sliceValue))
				idValue := modelValue.Field(idIndex).String()
				if idValue != "" {
					body := BuildQueryWithoutIdFromObject(sliceValue)
					jsonBody, err := json.Marshal(body)
					if err != nil {
						return successIndices, failureIndices, err
					}
					er1 := bi.Add(context.Background(), esutil.BulkIndexerItem{
						Action:     "create",
						DocumentID: idValue,
						Body:       strings.NewReader(string(jsonBody)), // esutil.NewJSONReader(body),
						OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
							successIds = append(successIds, res.DocumentID)
						},
						OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
							failIds = append(failIds, res.DocumentID)
						},
					})
					if er1 != nil {
						failureIndex = append(failureIndex, i)
					}
				} else {
					failureIndex = append(failureIndex, i)
				}
			} else {
				failureIndex = append(failureIndex, i)
			}
		}
		if er2 := bi.Close(context.Background()); er2 != nil {
			return successIndices, failureIndices, er2
		}
		successIndices, failureIndices = BuildIndicesResult(listIds, successIds, failIds)
		failureIndices = append(failureIndices, failureIndex...)
		return successIndices, failureIndices, nil
	}
	return successIndices, failureIndices, errors.New("invalid input")
}

func UpsertMany(ctx context.Context, es *elasticsearch.Client, indexName string, modelType reflect.Type, model interface{}) ([]int, []int, error) {
	value := reflect.Indirect(reflect.ValueOf(model))
	var failureIndex, successIndices, failureIndices []int
	if value.Kind() == reflect.Slice && value.Len() > 0 {
		bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:  indexName,
			Client: es,
		})
		if err != nil {
			return successIndices, failureIndices, err
		}
		listIds := FindListIdField(modelType, model)
		var successIds, failIds []interface{}
		for i := 0; i < value.Len(); i++ {
			sliceValue := value.Index(i).Interface()
			if idIndex, _, _ := FindIdField(modelType); idIndex >= 0 {
				modelValue := reflect.Indirect(reflect.ValueOf(sliceValue))
				idValue := modelValue.Field(idIndex).String()
				if idValue != "" {
					body := BuildQueryWithoutIdFromObject(sliceValue)
					jsonBody, err := json.Marshal(body)
					if err != nil {
						return successIndices, failureIndices, err
					}
					er1 := bi.Add(context.Background(), esutil.BulkIndexerItem{
						Action:     "index",
						DocumentID: idValue,
						Body:       strings.NewReader(string(jsonBody)), // esutil.NewJSONReader(body),
						OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
							successIds = append(successIds, res.DocumentID)
						},
						OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
							failIds = append(failIds, res.DocumentID)
						},
					})
					if er1 != nil {
						failureIndex = append(failureIndex, i)
					}
				} else {
					failureIndex = append(failureIndex, i)
				}
			} else {
				failureIndex = append(failureIndex, i)
			}
		}
		if er2 := bi.Close(context.Background()); er2 != nil {
			return successIndices, failureIndices, er2
		}
		successIndices, failureIndices = BuildIndicesResult(listIds, successIds, failIds)
		failureIndices = append(failureIndices, failureIndex...)
		return successIndices, failureIndices, nil
	}
	return successIndices, failureIndices, errors.New("invalid input")
}
func FindListIdField(modelType reflect.Type, model interface{}) (listIdS []interface{}) {
	value := reflect.Indirect(reflect.ValueOf(model))

	if value.Kind() == reflect.Slice {
		for i := 0; i < value.Len(); i++ {
			sliceValue := value.Index(i).Interface()
			if idIndex, _, _ := FindIdField(modelType); idIndex >= 0 {
				modelValue := reflect.Indirect(reflect.ValueOf(sliceValue))
				idValue := modelValue.Field(idIndex).String()
				listIdS = append(listIdS, idValue)
			}
		}
	}
	return
}
