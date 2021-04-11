package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/esutil"
	"reflect"
	"strings"
)

type DefaultSearchResultBuilder struct {
	BuildQuery        func(searchModel interface{}, resultModelType reflect.Type) map[string]interface{}
	BuildSort         func(s string, modelType reflect.Type) []string
	ExtractSearchInfo func(m interface{}) (string, int64, int64, int64, error)
}

func (b *DefaultSearchResultBuilder) Search(ctx context.Context, db *elasticsearch.Client, sm interface{}, modelType reflect.Type, indexName string) (interface{}, int64, error) {
	query := b.BuildQuery(sm, modelType)
	s, pageIndex, pageSize, firstPageSize, err := b.ExtractSearchInfo(sm)
	if err != nil {
		return nil, 0, err
	}
	var sort []string
	sort = b.BuildSort(s, modelType)
	return BuildSearchResult(ctx, db, modelType, indexName, query, sort, pageIndex, pageSize, firstPageSize)
}

func BuildSearchResult(ctx context.Context, db *elasticsearch.Client, modelType reflect.Type, indexName string, query map[string]interface{}, sort []string, pageIndex int64, pageSize int64, initPageSize int64) (interface{}, int64, error) {
	from := 0
	size := 0
	if initPageSize > 0 {
		if pageIndex == 1 {
			size = int(initPageSize)
		} else {
			from = int(pageSize*(pageIndex-2) + initPageSize)
			size = int(pageSize)
		}
	} else {
		from = int(pageSize * (pageIndex - 1))
		size = int(pageSize)
	}
	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  esutil.NewJSONReader(query),
		Sort:  sort,
		From:  &from,
		Size:  &size,
	}

	res, err := req.Do(ctx, db)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	results := reflect.New(modelsType).Interface()
	var count int64
	if res.IsError() {
		return nil, 0, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return nil, 0, err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			count = int64(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
			if err := json.NewDecoder(esutil.NewJSONReader(hits)).Decode(&results); err != nil {
				return nil, count, err
			}
		}
	}
	return results, count, nil
}

func BuildSort(s string, modelType reflect.Type) []string {
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
func GetSortType(sortType string) int {
	if sortType == "-" {
		return -1
	} else {
		return 1
	}
}
