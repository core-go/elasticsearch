package elasticsearch

import (
	"context"
	"reflect"

	"github.com/elastic/go-elasticsearch"
)

func NewSearchWriter(db *elasticsearch.Client, indexName string, modelType reflect.Type, search func(context.Context, interface{}) (interface{}, int64, error), options ...string) (*Searcher, *Writer) {
	var versionField string
	if len(options) >= 1 && len(options[0]) > 0 {
		versionField = options[0]
	}
	writer := NewWriter(db, indexName, modelType, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}

func NewSearchWriterWithVersion(db *elasticsearch.Client, indexName string, modelType reflect.Type, versionField string, search func(context.Context, interface{}) (interface{}, int64, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, indexName, modelType, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}

func NewDefaultSearchWriterWithVersion(db *elasticsearch.Client, indexName string, modelType reflect.Type, versionField string, options...func(m interface{}) (string, int64, int64, int64, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, indexName, modelType, versionField)
	searcher := NewDefaultSearcher(db, indexName, modelType, options...)
	return searcher, writer
}

func NewDefaultSearchWriter(db *elasticsearch.Client, indexName string, modelType reflect.Type, options...func(m interface{}) (string, int64, int64, int64, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, indexName, modelType, "")
	searcher := NewDefaultSearcher(db, indexName, modelType, options...)
	return searcher, writer
}
