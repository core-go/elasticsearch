package elasticsearch

import (
	es "github.com/elastic/go-elasticsearch/v8"
	"reflect"
)

func NewRepository(client *es.Client, indexName string, modelType reflect.Type, options ...string) *GenericWriter {
	return NewGenericWriterWithMapper(client, indexName, modelType, nil, options...)
}
func NewRepositoryWithMapper(client *es.Client, indexName string, modelType reflect.Type, mapper Mapper, options ...string) *GenericWriter {
	return NewGenericWriterWithMapper(client, indexName, modelType, mapper, options...)
}

func NewAdapter(client *es.Client, indexName string, modelType reflect.Type, options ...string) *Writer {
	return NewWriterWithMapper(client, indexName, modelType, nil, options...)
}
func NewAdapterWithMapper(client *es.Client, indexName string, modelType reflect.Type, mapper Mapper, options ...string) *Writer {
	return NewWriterWithMapper(client, indexName, modelType, mapper, options...)
}
