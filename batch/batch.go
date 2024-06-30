package batch

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	es "github.com/core-go/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type Model struct {
	Id   string                 `yaml:"id" mapstructure:"id" json:"id" gorm:"column:id;primary_key" bson:"_id" dynamodbav:"id" firestore:"-" avro:"id"`
	Body map[string]interface{} `yaml:"body" mapstructure:"body" json:"body" gorm:"column:body" bson:"body" dynamodbav:"body" firestore:"body" avro:"body"`
	Data string                 `yaml:"data" mapstructure:"data" json:"data" gorm:"column:data" bson:"data" dynamodbav:"data" firestore:"data" avro:"data"`
}

func BuildModels[T any](objs []T, idx int, FieldMap []es.FieldMap) ([]Model, error) {
	models := make([]Model, 0)
	le := len(objs)
	if le <= 0 {
		return models, nil
	}
	for i := 0; i < le; i++ {
		obj := objs[i]
		vo := reflect.ValueOf(obj)
		if vo.Kind() == reflect.Ptr {
			vo = reflect.Indirect(vo)
		}
		id := vo.Field(idx).Interface().(string)
		body := es.BuildBody(obj, FieldMap)
		model := Model{Id: id, Body: body}
		data, err := json.Marshal(model.Body)
		if err != nil {
			return nil, err
		}
		model.Data = string(data)
		models = append(models, model)
	}
	return models, nil
}

func CreateBatch(ctx context.Context, client *elasticsearch.Client, index string, objs []Model) ([]int, error) {
	failIndices := make([]int, 0)
	indexer, er0 := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  index,
		Client: client,
	})
	if er0 != nil {
		return nil, er0
	}
	le := len(objs)
	for i := 0; i < le; i++ {
		obj := objs[i]
		isAdded := false
		er1 := indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "create",
			DocumentID: obj.Id,
			Body:       strings.NewReader(obj.Data),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				failIndices = append(failIndices, i)
				isAdded = true
			},
		})
		if er1 != nil {
			if !isAdded {
				failIndices = append(failIndices, i)
			}
		}
	}
	er2 := indexer.Close(ctx)
	return failIndices, er2
}

func UpdateBatch(ctx context.Context, client *elasticsearch.Client, index string, objs []Model) ([]int, error) {
	failIndices := make([]int, 0)
	indexer, er0 := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  index,
		Client: client,
	})
	if er0 != nil {
		return nil, er0
	}
	le := len(objs)
	for i := 0; i < le; i++ {
		obj := objs[i]
		isAdded := false
		er1 := indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: obj.Id,
			Body:       strings.NewReader(obj.Data),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				failIndices = append(failIndices, i)
				isAdded = true
			},
		})
		if er1 != nil {
			if !isAdded {
				failIndices = append(failIndices, i)
			}
		}
	}
	er2 := indexer.Close(ctx)
	return failIndices, er2
}

func SaveBatch(ctx context.Context, client *elasticsearch.Client, index string, objs []Model) ([]int, error) {
	failIndices := make([]int, 0)
	indexer, er0 := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  index,
		Client: client,
	})
	if er0 != nil {
		return nil, er0
	}
	le := len(objs)
	for i := 0; i < le; i++ {
		obj := objs[i]
		isAdded := false
		er1 := indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "index",
			DocumentID: obj.Id,
			Body:       strings.NewReader(obj.Data),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				failIndices = append(failIndices, i)
				isAdded = true
			},
		})
		if er1 != nil {
			if !isAdded {
				failIndices = append(failIndices, i)
			}
		}
	}
	er2 := indexer.Close(ctx)
	return failIndices, er2
}
