package query

import (
	"github.com/core-go/search"
	"reflect"
	"strings"
	"time"
)

type Builder struct {
	ModelType reflect.Type
}

func NewBuilder(resultModelType reflect.Type) *Builder {
	return &Builder{ModelType: resultModelType}
}
func (b *Builder) BuildQuery(sm interface{}) map[string]interface{} {
	return Build(sm, b.ModelType)
}

func Build(sm interface{}, resultModelType reflect.Type) map[string]interface{} {
	query := map[string]interface{}{}
	if _, ok := sm.(*search.Filter); ok {
		return query
	}
	value := reflect.Indirect(reflect.ValueOf(sm))
	numField := value.NumField()
	for i := 0; i < numField; i++ {
		fieldValue := value.Field(i).Interface()
		if v, ok := fieldValue.(*search.Filter); ok {
			if v.Excluding != nil && len(v.Excluding) > 0 {
				_, _, columnName := getFieldByBson(value.Type(), "_id")
				if len(columnName) > 0 {
					actionDateQuery := map[string]interface{}{}
					actionDateQuery["$nin"] = v.Excluding
					query[columnName] = actionDateQuery
				}
			}
			continue
		} else if rangeDate, ok := fieldValue.(search.DateRange); ok {
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeDate.Min
			query[columnName] = actionDateQuery
			var eDate = rangeDate.Max.Add(time.Hour * 24)
			rangeDate.Max = &eDate
			actionDateQuery["$lte"] = rangeDate.Max
			query[columnName] = actionDateQuery
		} else if rangeDate, ok := fieldValue.(*search.DateRange); ok && rangeDate != nil {
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeDate.Min
			query[columnName] = actionDateQuery
			var eDate = rangeDate.Max.Add(time.Hour * 24)
			rangeDate.Max = &eDate
			actionDateQuery["$lte"] = rangeDate.Max
			query[columnName] = actionDateQuery
		} else if rangeTime, ok := fieldValue.(search.TimeRange); ok {
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeTime.StartTime
			query[columnName] = actionDateQuery
			actionDateQuery["$lt"] = rangeTime.EndTime
			query[columnName] = actionDateQuery
		} else if rangeTime, ok := fieldValue.(*search.TimeRange); ok && rangeTime != nil {
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)

			actionDateQuery := map[string]interface{}{}

			actionDateQuery["$gte"] = rangeTime.StartTime
			query[columnName] = actionDateQuery
			actionDateQuery["$lt"] = rangeTime.EndTime
			query[columnName] = actionDateQuery
		} else if numberRange, ok := fieldValue.(search.NumberRange); ok {
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)
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
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)
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
			_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)
			actionDateQuery["$in"] = fieldValue
			query[columnName] = actionDateQuery
		} else {
			t := value.Field(i).Kind().String()
			if _, ok := fieldValue.(*search.Filter); t == "bool" || (strings.Contains(t, "int") && fieldValue != 0) || (strings.Contains(t, "float") && fieldValue != 0) || (!ok && t == "string" && value.Field(i).Len() > 0) || (!ok && t == "ptr" &&
				value.Field(i).Pointer() != 0) {
				_, columnName := findFieldByName(resultModelType, value.Type().Field(i).Name)
				if len(columnName) > 0 {
					query[columnName] = fieldValue
				}
			}
		}
	}
	return query
}

func findFieldByName(modelType reflect.Type, fieldName string) (index int, jsonTagName string) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if field.Name == fieldName {
			jsonTagName := fieldName
			if jsonTag, ok := field.Tag.Lookup("json"); ok {
				jsonTagName = strings.Split(jsonTag, ",")[0]
			}
			return index, jsonTagName
		}
	}
	return -1, fieldName
}
func getFieldByBson(modelType reflect.Type, bsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("bson")
		if ok1 && strings.Split(tag1, ",")[0] == bsonName {
			if tag2, ok2 := field.Tag.Lookup("json"); ok2 {
				json := strings.Split(tag2, ",")[0]
				return i, field.Name, json
			}
			return i, field.Name, ""
		}
	}
	return -1, bsonName, bsonName
}
