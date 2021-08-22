package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"strings"
)

func BuildSearchResult(ctx context.Context, db *elasticsearch.Client, results interface{}, indexName string, query map[string]interface{}, sort []string, limit int64, offset int64, options...func(context.Context, interface{}) (interface{}, error)) (int64, error) {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 {
		mp = options[0]
	}

	from := int(offset)
	size := int(limit)
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  esutil.NewJSONReader(query),
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
			err := json.NewDecoder(esutil.NewJSONReader(hits)).Decode(results) // err := json.NewDecoder(esutil.NewJSONReader(hits)).Decode(&results)
			if err != nil {
				return count, err
			}
			if mp != nil {
				MapModels(ctx, results, mp)
			}
			return count, err
		}
	}
	return count, nil
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
		}
		sort = append(sort, fieldName)
	}
	return sort
}
