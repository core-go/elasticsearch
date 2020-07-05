package elasticsearch

import (
	"github.com/common-go/search"
	"reflect"
	"strings"
)

const desc = "DESC"

type DefaultSortBuilder struct {
}

func (b *DefaultSortBuilder) BuildSort(s search.SearchModel, modelType reflect.Type) []string {
	var sort []string
	if len(s.SortField) == 0 {
		return sort
	}
	if strings.Index(s.SortField, ",") < 0 {
		sort = append(sort, s.SortField)
	} else {
		sorts := strings.Split(s.SortField, ",")
		for i := 0; i < len(sorts); i++ {
			sortField := strings.TrimSpace(sorts[i])
			params := strings.Split(sortField, " ")
			if len(params) > 0 {
				sort = append(sort, params[0])
			}
		}
	}
	return sort
}

func (b *DefaultSortBuilder) getSortType(sortType string) int {
	if strings.ToUpper(sortType) != strings.ToUpper(desc) {
		return 1
	}
	return -1
}
