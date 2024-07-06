package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func FindIdField(modelType reflect.Type) (int, string, string) {
	return findBsonField(modelType, "_id")
}
func findBsonField(modelType reflect.Type, bsonName string) (int, string, string) {
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
func FindFieldByName(modelType reflect.Type, fieldName string) (index int, jsonTagName string) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if field.Name == fieldName {
			jsonTagName := fieldName
			if jsonTag, ok := field.Tag.Lookup("json"); ok {
				jsonTagName = strings.Split(jsonTag, ",")[0]
			}
			return index, jsonTagName
		}
	}
	return -1, fieldName
}

func FindFieldByJson(modelType reflect.Type, jsonTagName string) (index int, fieldName string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonTagName {
			return i, field.Name
		}
	}
	return -1, jsonTagName
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

func Exist(ctx context.Context, es *elasticsearch.Client, index string, documentID string) (bool, error) {
	req := esapi.ExistsRequest{
		Index:      index,
		DocumentID: documentID,
	}
	res, err := req.Do(ctx, es)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return false, err
		} else {
			return r["found"].(bool), nil
		}
	}
}
func FindOne(ctx context.Context, client *elasticsearch.Client, index string, documentID string, result interface{}, idJson string) (bool, error) {
	return FindOneWithVersion(ctx, client, index, documentID, result, idJson, "")
}
func FindOneWithVersion(ctx context.Context, client *elasticsearch.Client, index string, documentID string, result interface{}, idJson string, versionJson string) (bool, error) {
	req := esapi.GetRequest{
		Index:      index,
		DocumentID: documentID,
	}
	res, err := req.Do(ctx, client)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if !res.IsError() {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err == nil {
			hit := r["_source"].(map[string]interface{})
			if len(idJson) > 0 {
				hit[idJson] = r["_id"]
			}
			if len(versionJson) > 0 {
				hit[versionJson] = r["_version"]
			}
			if err := json.NewDecoder(esutil.NewJSONReader(hit)).Decode(&result); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	return false, errors.New("response error")
}
func Find(ctx context.Context, client *elasticsearch.Client, index []string, query map[string]interface{}, result interface{}, idJson string) error {
	return FindWithVersion(ctx, client, index, query, result, idJson, "")
}
func FindWithVersion(ctx context.Context, client *elasticsearch.Client, index []string, query map[string]interface{}, result interface{}, idJson string, versionJson string) error {
	req := esapi.SearchRequest{
		Index:          index,
		Body:           esutil.NewJSONReader(query),
		TrackTotalHits: true,
		Pretty:         true,
	}
	res, err := req.Do(ctx, client)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		return errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			listResults := make([]interface{}, 0)
			for _, hitObj := range hits {
				hit := hitObj.(map[string]interface{})
				r := hit["_source"]
				rs := r.(map[string]interface{})
				if len(idJson) > 0 {
					rs[idJson] = hit["_id"]
				}
				if len(versionJson) > 0 {
					hit[versionJson] = hit["_version"]
				}
				listResults = append(listResults, r)
			}
			return json.NewDecoder(esutil.NewJSONReader(listResults)).Decode(result)
		}
	}
}
