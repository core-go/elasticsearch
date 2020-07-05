package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/common-go/search"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/esutil"
	"reflect"
)

type DefaultSearchResultBuilder struct {
	QueryBuilder QueryBuilder
	SortBuilder  SortBuilder
}

func (b *DefaultSearchResultBuilder) BuildSearchResult(ctx context.Context, db *elasticsearch.Client, sm interface{}, modelType reflect.Type, indexName string) (*search.SearchResult, error) {
	query := b.QueryBuilder.BuildQuery(sm, modelType)

	var sort []string
	var searchModel *search.SearchModel

	if sModel, ok := sm.(*search.SearchModel); ok {
		searchModel = sModel
		sort = b.SortBuilder.BuildSort(*sModel, modelType)
	} else {
		value := reflect.Indirect(reflect.ValueOf(sm))
		numField := value.NumField()
		for i := 0; i < numField; i++ {
			if sModel1, ok := value.Field(i).Interface().(*search.SearchModel); ok {
				searchModel = sModel1
				sort = b.SortBuilder.BuildSort(*sModel1, modelType)
			}
		}
	}
	return b.Build(ctx, db, modelType, indexName, query, sort, searchModel.PageIndex, searchModel.PageSize, searchModel.InitPageSize)
}

func (b *DefaultSearchResultBuilder) Build(ctx context.Context, db *elasticsearch.Client, modelType reflect.Type, indexName string, query map[string]interface{}, sort []string, pageIndex int64, pageSize int64, initPageSize int64) (*search.SearchResult, error) {
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
		return nil, err
	}
	defer res.Body.Close()
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	results := reflect.New(modelsType).Interface()
	var count int64
	if res.IsError() {
		return nil, errors.New("response error")
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			return nil, err
		} else {
			hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
			count = int64(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64))
			if err := json.NewDecoder(esutil.NewJSONReader(hits)).Decode(&results); err != nil {
				return nil, err
			}
		}
	}

	searchResult := search.SearchResult{}
	searchResult.ItemTotal = count

	searchResult.LastPage = false
	lengthModels := int64(reflect.Indirect(reflect.ValueOf(results)).Len())
	var receivedItems int64
	if initPageSize > 0 {
		if pageIndex == 1 {
			receivedItems = initPageSize
		} else if pageIndex > 1 {
			receivedItems = pageSize*(pageIndex-2) + initPageSize + lengthModels
		}
	} else {
		receivedItems = pageSize*(pageIndex-1) + lengthModels
	}
	searchResult.LastPage = receivedItems >= count

	searchResult.Results = results

	return &searchResult, nil
}
