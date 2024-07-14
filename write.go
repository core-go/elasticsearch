package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type FieldMap struct {
	Index     int
	Json      string
	OmitEmpty bool
	Id        bool
}

func BuildMap(modelType reflect.Type) []FieldMap {
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	var fms []FieldMap
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		json := field.Name
		omitEmpty := false
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			if tag1 != "-" {
				jsonTags := strings.Split(tag1, ",")
				json = jsonTags[0]
				for _, tag := range jsonTags {
					if strings.TrimSpace(tag) == "omitempty" {
						omitEmpty = true
					}
				}
				fm := FieldMap{Index: i, Json: json, OmitEmpty: omitEmpty}
				bsonTag := field.Tag.Get("bson")
				tags := strings.Split(bsonTag, ",")
				for _, tag := range tags {
					if strings.TrimSpace(tag) == "_id" {
						fm.Id = true
					}
				}
				fms = append(fms, fm)
			}
		}
	}
	return fms
}
func BuildBody(model interface{}, fields []FieldMap) map[string]interface{} {
	vo := reflect.ValueOf(model)
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	res := map[string]interface{}{}
	le := len(fields)
	for i := 0; i < le; i++ {
		f := vo.Field(fields[i].Index)
		if !fields[i].Id {
			if fields[i].OmitEmpty {
				if f.Kind() == reflect.Ptr {
					if !f.IsNil() {
						res[fields[i].Json] = f.Interface()
					}
				} else {
					v := f.Interface()
					s, ok := v.(string)
					if ok {
						if len(s) > 0 {
							res[fields[i].Json] = s
						}
					} else {
						res[fields[i].Json] = f.Interface()
					}
				}
			} else {
				res[fields[i].Json] = f.Interface()
			}
		}
	}
	return res
}

func Create(ctx context.Context, es *elasticsearch.Client, index string, model interface{}, id string) (int64, error) {
	var req esapi.CreateRequest
	if len(id) > 0 {
		req = esapi.CreateRequest{
			Index:      index,
			DocumentID: id,
			Body:       esutil.NewJSONReader(model),
			Refresh:    "true",
		}
	} else {
		req = esapi.CreateRequest{
			Index:   index,
			Body:    esutil.NewJSONReader(model),
			Refresh: "true",
		}
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return 0, nil
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
			return int64(r["_version"].(float64)), nil
		}
	}
}

func Update(ctx context.Context, es *elasticsearch.Client, index string, model interface{}, id string) (int64, error) {
	query := map[string]interface{}{
		"doc": model,
	}
	req := esapi.UpdateRequest{
		Index:      index,
		DocumentID: id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
			return successful, nil
		}
	}
}

func Save(ctx context.Context, es *elasticsearch.Client, index string, model interface{}, id string) (int64, error) {
	query := map[string]interface{}{
		"doc": model,
	}
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	}
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return -1, err
	}
	successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
	return successful, nil
}

func Patch(ctx context.Context, es *elasticsearch.Client, index string, model map[string]interface{}, idName string) (int64, error) {
	idValue, ok := model[idName]
	if !ok {
		return -1, fmt.Errorf("%s must be in map[string]interface{} for patch", idName)
	}
	id, ok2 := idValue.(string)
	if !ok2 {
		return -1, fmt.Errorf("%s map[string]interface{} must be a string for patch", idName)
	}
	delete(model, idName)
	query := map[string]interface{}{
		"doc": model,
	}
	req := esapi.UpdateRequest{
		Index:      index,
		DocumentID: id,
		Body:       esutil.NewJSONReader(query),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, es)
	model[idName] = id
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
			return successful, nil
		}
	}
}

func Delete(ctx context.Context, es *elasticsearch.Client, index string, documentID string) (int64, error) {
	req := esapi.DeleteRequest{
		Index:      index,
		DocumentID: documentID,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return -1, err
	}
	defer res.Body.Close()
	if res.IsError() {
		return -1, errors.New("document ID not exists in the index")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return -1, err
		} else {
			successful := int64(r["_shards"].(map[string]interface{})["successful"].(float64))
			return successful, nil
		}
	}
}

func DeleteBatch(ctx context.Context, client *elasticsearch.Client, index string, ids []string) ([]string, error) {
	indexer, er0 := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  index,
		Client: client,
	})
	if er0 != nil {
		return nil, er0
	}
	var er2 error
	failIds := make([]string, 0)
	le := len(ids)
	for i := 0; i < le; i++ {
		er1 := indexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action:     "delete",
			DocumentID: ids[i],
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				failIds = append(failIds, res.DocumentID)
			},
		})
		if er1 != nil && er2 == nil {
			er2 = er1
		}
	}
	er3 := indexer.Close(ctx)
	if er3 != nil && er2 == nil {
		er2 = er3
	}
	return failIds, er2
}
