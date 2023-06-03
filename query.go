package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"reflect"
	"strings"
)

func BuildSearchResult(ctx context.Context, db *elasticsearch.Client, results interface{}, indexName string, query map[string]interface{}, sort []string, limit int64, offset int64, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) (int64, error) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}

	from := int(offset)
	size := int(limit)
	fullQuery := UpdateQuery(query)
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  esutil.NewJSONReader(fullQuery),
		Sort:  sort,
		From:  &from,
		Size:  &size,
	}

	res, err := req.Do(ctx, db)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	var count int64
	if res.IsError() {
		return 0, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return 0, err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			count = int64(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
			listResults := make([]interface{}, 0)
			for _, hit := range hits {
				r := hit.(map[string]interface{})["_source"]
				r.(map[string]interface{})["id"] = hit.(map[string]interface{})["_id"]
				stValue := reflect.New(modelType).Elem()
				for i := 0; i < modelType.NumField(); i++ {
					field := modelType.Field(i)
					if value, ok := r.(map[string]interface{})[field.Name]; ok {
						stValue.Field(i).Set(reflect.ValueOf(value))
					}
				}
				listResults = append(listResults, r)
			}

			err := json.NewDecoder(esutil.NewJSONReader(listResults)).Decode(results)
			if err != nil {
				return count, err
			}

			if mp != nil {
				MapModels(ctx, results, mp)
			}

			return count, err
		}
	}
}

func BuildSort(s string) []string {
	var sort []string
	if len(s) == 0 {
		return sort
	}
	sorts := strings.Split(s, ",")
	for i := 0; i < len(sorts); i++ {
		sortField := strings.TrimSpace(sorts[i])
		fieldName := sortField
		c := sortField[0:1]
		if c == "-" || c == "+" {
			fieldName = sortField[1:]
			if c == "-" {
				fieldName += ":desc"
			}
		}
		sort = append(sort, fieldName)
	}
	return sort
}
