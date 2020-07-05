package elasticsearch

import (
	"reflect"
)

type QueryBuilder interface {
	BuildQuery(searchModel interface{}, resultModelType reflect.Type) map[string]interface{}
}
