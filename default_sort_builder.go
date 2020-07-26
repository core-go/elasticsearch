package elasticsearch

import (
	"github.com/common-go/search"
	"reflect"
	"strings"
)

type DefaultSortBuilder struct {
}

func (b *DefaultSortBuilder) BuildSort(s search.SearchModel, modelType reflect.Type) []string {
	var sort []string
	if len(s.Sort) == 0 {
		return sort
	}
	sorts := strings.Split(s.Sort, ",")
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

func (b *DefaultSortBuilder) getSortType(sortType string) int {
	if sortType == "-" {
		return -1
	} else  {
		return 1
	}
}
