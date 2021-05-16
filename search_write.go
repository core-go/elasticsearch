package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v7"
	"reflect"
)

func NewDefaultSearchWriter(client *elasticsearch.Client, indexName string, modelType reflect.Type, search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error), options ...string) (*Searcher, *Writer) {
	return NewDefaultSearchWriterWithMapper(client, indexName, modelType, search, nil, options...)
}
func NewDefaultSearchWriterWithMapper(client *elasticsearch.Client, indexName string, modelType reflect.Type, search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error), mapper Mapper, options ...string) (*Searcher, *Writer) {
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	writer := NewWriterWithMapper(client, indexName, modelType, mapper, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}
func NewSearchWriter(client *elasticsearch.Client, indexName string, modelType reflect.Type, buildQuery func(interface{}) map[string]interface{}, getSort func(m interface{}) string, options ...string) (*Searcher, *Writer) {
	return NewSearchWriterWithMapper(client, indexName, modelType, buildQuery, getSort, nil, options...)
}
func NewSearchWriterWithMapper(client *elasticsearch.Client, indexName string, modelType reflect.Type, buildQuery func(interface{}) map[string]interface{}, getSort func(m interface{}) string, mapper Mapper, options ...string) (*Searcher, *Writer) {
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	writer := NewWriterWithMapper(client, indexName, modelType, mapper, versionField)
	var searcher *Searcher
	if mapper != nil {
		searcher = NewSearcherWithQuery(client, indexName, buildQuery, getSort, mapper.DbToModel)
	} else {
		searcher = NewSearcherWithQuery(client, indexName, buildQuery, getSort)
	}
	return searcher, writer
}
