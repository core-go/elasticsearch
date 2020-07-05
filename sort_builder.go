package elasticsearch

import (
	"github.com/common-go/search"
	"reflect"
)

type SortBuilder interface {
	BuildSort(searchModel search.SearchModel, modelType reflect.Type) []string
}
