package elasticsearch

import (
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/common-go/search"
)

type QueryBuilder struct {
	ModelType reflect.Type
}

func NewQueryBuilder(resultModelType reflect.Type) *QueryBuilder {
	return &QueryBuilder{ModelType: resultModelType}
}
func (b *QueryBuilder) BuildQuery(sm interface{}) map[string]interface{} {
	return BuildQuery(sm, b.ModelType)
}

func BuildQuery(sm interface{}, resultModelType reflect.Type) map[string]interface{} {
	query := map[string]interface{}{}
	if _, ok := sm.(*search.SearchModel); ok {
		return query
	}
	value := reflect.Indirect(reflect.ValueOf(sm))
	numField := value.NumField()
	for i := 0; i < numField; i++ {
		fieldValue := value.Field(i).Interface()
		if v, ok := fieldValue.(*search.SearchModel); ok {
			if len(v.Excluding) > 0 {
				for key, val := range v.Excluding {
					if len(val) > 0 {
						actionDateQuery := map[string]interface{}{}
						actionDateQuery["$nin"] = val
						query[key] = actionDateQuery
					}
				}
			}
			continue
		} else if rangeDate, ok := fieldValue.(search.DateRange); ok {
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeDate.StartDate
			query[columnName] = actionDateQuery
			var eDate = rangeDate.EndDate.Add(time.Hour * 24)
			rangeDate.EndDate = &eDate
			actionDateQuery["$lte"] = rangeDate.EndDate
			query[columnName] = actionDateQuery
		} else if rangeDate, ok := fieldValue.(*search.DateRange); ok && rangeDate != nil {
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeDate.StartDate
			query[columnName] = actionDateQuery
			var eDate = rangeDate.EndDate.Add(time.Hour * 24)
			rangeDate.EndDate = &eDate
			actionDateQuery["$lte"] = rangeDate.EndDate
			query[columnName] = actionDateQuery
		} else if rangeTime, ok := fieldValue.(search.TimeRange); ok {
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeTime.StartTime
			query[columnName] = actionDateQuery
			actionDateQuery["$lt"] = rangeTime.EndTime
			query[columnName] = actionDateQuery
		} else if rangeTime, ok := fieldValue.(*search.TimeRange); ok && rangeTime != nil {
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeTime.StartTime
			query[columnName] = actionDateQuery
			actionDateQuery["$lt"] = rangeTime.EndTime
			query[columnName] = actionDateQuery
		} else if numberRange, ok := fieldValue.(search.NumberRange); ok {
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)
			amountQuery := map[string]interface{}{}

			if numberRange.Min != nil {
				amountQuery["$gte"] = *numberRange.Min
			} else if numberRange.Lower != nil {
				amountQuery["$gt"] = *numberRange.Lower
			}
			if numberRange.Max != nil {
				amountQuery["$lte"] = *numberRange.Max
			} else if numberRange.Upper != nil {
				amountQuery["$lt"] = *numberRange.Upper
			}

			if len(amountQuery) > 0 {
				query[columnName] = amountQuery
			}
		} else if numberRange, ok := fieldValue.(*search.NumberRange); ok && numberRange != nil {
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)
			amountQuery := map[string]interface{}{}

			if numberRange.Min != nil {
				amountQuery["$gte"] = *numberRange.Min
			} else if numberRange.Lower != nil {
				amountQuery["$gt"] = *numberRange.Lower
			}
			if numberRange.Max != nil {
				amountQuery["$lte"] = *numberRange.Max
			} else if numberRange.Upper != nil {
				amountQuery["$lt"] = *numberRange.Upper
			}

			if len(amountQuery) > 0 {
				query[columnName] = amountQuery
			}
		} else if value.Field(i).Kind().String() == "slice" {
			actionDateQuery := map[string]interface{}{}
			_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)
			actionDateQuery["$in"] = fieldValue
			query[columnName] = actionDateQuery
		} else {
			t := value.Field(i).Kind().String()
			if _, ok := fieldValue.(*search.SearchModel); t == "bool" || (strings.Contains(t, "int") && fieldValue != 0) || (strings.Contains(t, "float") && fieldValue != 0) || (!ok && t == "string" && value.Field(i).Len() > 0) || (!ok && t == "ptr" &&
				value.Field(i).Pointer() != 0) {
				_, columnName := FindFieldByName(resultModelType, value.Type().Field(i).Name)
				if len(columnName) > 0 {
					query[columnName] = fieldValue
				}
			}
		}
	}
	return query
}

func ExtractSearchInfo(m interface{}) (string, int64, int64, int64, error) {
	if sModel, ok := m.(*search.SearchModel); ok {
		return sModel.Sort, sModel.Page, sModel.Limit, sModel.FirstLimit, nil
	} else {
		value := reflect.Indirect(reflect.ValueOf(m))
		numField := value.NumField()
		for i := 0; i < numField; i++ {
			if sModel1, ok := value.Field(i).Interface().(*search.SearchModel); ok {
				return sModel1.Sort, sModel1.Page, sModel1.Limit, sModel1.FirstLimit, nil
			}
		}
		return "", 0, 0, 0, errors.New("cannot extract sort, pageIndex, pageSize, firstPageSize from model")
	}
}
