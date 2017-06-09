package memj

import (
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// Limit constants
const (
	NoLimit = iota
	FindOne
)

// Comparison operator constants
const (
	EQ  = "$eq"
	GT  = "$gt"
	GTE = "$gte"
	LT  = "$lt"
	LTE = "$lte"
	NE  = "$ne"
	IN  = "$in"
	NIN = "$nin"
)

// Logical operator constants
const (
	AND = "$and"
	OR  = "$or"
)

// MemJ - memory json
type MemJ struct {
	mutexLock       sync.RWMutex
	collectionLocks map[string]*sync.RWMutex
	data            map[string][]map[string]interface{}
}

// New - create new instance of MemJ
func New() (*MemJ, error) {
	memj := &MemJ{
		collectionLocks: make(map[string]*sync.RWMutex),
		data:            make(map[string][]map[string]interface{}),
	}

	return memj, nil
}

// Insert - insert json payload to collection
func (m *MemJ) Insert(collection string, payload map[string]interface{}) (string, error) {
	lock := m.getCollectionLock(collection)

	lock.Lock()
	defer lock.Unlock()

	objectID := uuid.New().String()
	payload["objectid"] = objectID
	m.data[collection] = append(m.data[collection], payload)

	return objectID, nil
}

// Find - find collection with objectId in collection
func (m *MemJ) Find(collection, objectID string) (map[string]interface{}, error) {
	lock := m.getCollectionLock(collection)

	lock.RLock()
	defer lock.RUnlock()

	for _, value := range m.data[collection] {
		if value["objectid"] == objectID {
			return value, nil
		}
	}

	return nil, errors.New("Not found")
}

// FindAll - return all documents in the collection
func (m *MemJ) FindAll(collection string) ([]map[string]interface{}, error) {
	lock := m.getCollectionLock(collection)

	lock.RLock()
	defer lock.RUnlock()

	return m.data[collection], nil
}

// Update - update existing object identified by objectID
func (m *MemJ) Update(collection, objectID string, payload map[string]interface{}) (bool, error) {
	lock := m.getCollectionLock(collection)

	lock.Lock()
	defer lock.Unlock()

	for index, value := range m.data[collection] {
		if value["objectid"] == objectID {
			return m.updateFields(collection, index, payload)
		}
	}

	return false, errors.New("Not found")
}

func (m *MemJ) updateFields(collection string, index int, payload map[string]interface{}) (bool, error) {
	document := m.data[collection][index]
	for k, v := range payload {
		queryParts := strings.Split(k, ".")
		queryPartsLen := len(queryParts)
		if queryPartsLen == 1 {
			document[k] = v
		} else {
			subDocument := document
			for index, key := range queryParts {
				if queryPartsLen == index+1 {
					subDocument[key] = v
				} else {
					var ok bool
					subDocument, ok = subDocument[key].(map[string]interface{})
					if !ok {
						return false, errors.New("Invalid field path")
					}
				}
			}
		}
	}
	return true, nil
}

// Delete - delete object in collection identified by objectID
func (m *MemJ) Delete(collection, objectID string) (bool, error) {
	lock := m.getCollectionLock(collection)

	lock.Lock()
	defer lock.Unlock()

	for index, value := range m.data[collection] {
		if value["objectid"] == objectID {
			m.data[collection] = append(m.data[collection][:index], m.data[collection][index+1:]...)
			return true, nil
		}
	}

	return false, errors.New("Not found")
}

// Query - query for object in collection
func (m *MemJ) Query(collection string, query map[string]interface{}, limit int) ([]map[string]interface{}, error) {
	maxLimit := 0

	lock := m.getCollectionLock(collection)

	lock.RLock()
	defer lock.RUnlock()

	var result []map[string]interface{}

	for _, value := range m.data[collection] {
		isFound, err := m.performMatchQuery(query, value)
		if err != nil {
			return nil, err
		}

		if isFound {
			result = append(result, value)
			if limit != 0 {
				maxLimit++
				if maxLimit >= limit {
					return result, nil
				}
			}
		}
	}

	return result, nil
}

func (m *MemJ) performMatchQuery(query, document map[string]interface{}) (bool, error) {
	var compareValue interface{}
	var err error
	isFound := false
	for k := range query {
		key := strings.Split(k, ".")
		if len(key) == 1 {
			if m.isLogicalOperator(k) {
				queryList, ok := query[k].([]interface{})
				if !ok {
					return false, errors.New("Logical operator query has invalid syntax.  Expected a list of queries.")
				}
				isFound, err = m.performLogicalOp(k, queryList, document)
				break
			} else {
				var opType string
				var compareToValue interface{}
				var isComparison bool
				opType, compareToValue, isComparison, err = m.isComparisonOperator(query[k])
				if err != nil {
					return false, err
				}
				if isComparison {
					docValue := document[k]
					isFound, err = m.performComperisonOp(opType, docValue, compareToValue)
					if err != nil {
						return false, err
					}
					break
				} else {
					compareValue = document[k]
				}
			}
		} else {
			compareValue = m.getNestedQueryValue(key, document)
		}
		if query[k] == compareValue {
			isFound = true
		} else {
			isFound = false
			break
		}
	}

	return isFound, err
}

func (m *MemJ) performComperisonOp(op string, compVal1, compVal2 interface{}) (bool, error) {
	if reflect.TypeOf(compVal1) != reflect.TypeOf(compVal2) {
		return false, errors.New("Cannot compare values of different types")
	}

	switch compVal1.(type) {
	case string:
		compVal1Str, _ := compVal1.(string)
		compVal2Str, _ := compVal2.(string)
		isFound := m.compareStrings(op, compVal1Str, compVal2Str)
		return isFound, nil

	case float64:
		compVal1Float, _ := compVal1.(float64)
		compVal2Float, _ := compVal2.(float64)
		isFound := m.compareFloats(op, compVal1Float, compVal2Float)
		return isFound, nil
	}

	return false, nil
}

func (m *MemJ) compareFloats(op string, compVal1, compVal2 float64) bool {
	switch op {
	case GT:
		return compVal1 > compVal2

	case GTE:
		return compVal1 >= compVal2

	case LT:
		return compVal1 < compVal2

	case LTE:
		return compVal1 <= compVal2

	case NE:
		return compVal1 != compVal2

	case EQ:
		return compVal1 == compVal2
	}

	return false
}

func (m *MemJ) compareStrings(op, compVal1, compVal2 string) bool {
	switch op {
	case GT:
		return compVal1 > compVal2

	case GTE:
		return compVal1 >= compVal2

	case LT:
		return compVal1 < compVal2

	case LTE:
		return compVal1 <= compVal2

	case NE:
		return compVal1 != compVal2

	case EQ:
		return compVal1 == compVal2
	}

	return false
}

func (m *MemJ) performLogicalOp(operator string,
	queryList []interface{},
	document map[string]interface{}) (bool, error) {

	var opSuccessList []bool
	for _, query := range queryList {
		queryMap, _ := query.(map[string]interface{})
		isFound, err := m.performMatchQuery(queryMap, document)
		if err != nil {
			return false, err
		}

		opSuccessList = append(opSuccessList, isFound)
	}

	var isSuccess bool
	switch operator {
	case OR:
		isSuccess = m.any(opSuccessList)
		break

	case AND:
		isSuccess = m.all(opSuccessList)
	}

	return isSuccess, nil
}

func (m *MemJ) isComparisonOperator(op interface{}) (string, interface{}, bool, error) {
	opType, ok := op.(map[string]interface{})
	if !ok {
		return "", nil, false, nil
	}

	if len(opType) == 0 || len(opType) > 1 {
		return "", nil, false, nil
	}

	for k, v := range opType {
		switch k {
		case EQ, GT, GTE, LT, LTE, NE:
			switch v := v.(type) {
			case float64, string:
				return k, v, true, nil

			default:
				return "", nil, false, errors.New("Invalid type for comparison")
			}

			// TODO : this is not implemented yet
			/*case IN, NIN:
			  return k, v, true, nil*/
		}
	}

	return "", nil, false, nil
}

func (m *MemJ) isLogicalOperator(key string) bool {
	if key == OR || key == AND {
		return true
	}
	return false
}

func (m *MemJ) all(logicList []bool) bool {
	for _, b := range logicList {
		if !b {
			return false
		}
	}
	return true
}

func (m *MemJ) any(logicList []bool) bool {
	for _, b := range logicList {
		if b {
			return true
		}
	}
	return false
}

func (m *MemJ) getNestedQueryValue(nestedKeys []string, document map[string]interface{}) interface{} {
	var currentValue interface{}
	var documentLevel interface{} = document
	for _, key := range nestedKeys {
		currentDocument, ok := documentLevel.(map[string]interface{})
		if !ok {
			return nil
		}

		currentValue = currentDocument[key]
		documentLevel = currentDocument[key]
	}

	return currentValue
}

func (m *MemJ) getCollectionLock(collection string) *sync.RWMutex {
	m.mutexLock.RLock()

	cl, ok := m.collectionLocks[collection]
	m.mutexLock.RUnlock()

	if !ok {
		cl = &sync.RWMutex{}
		m.mutexLock.Lock()
		m.collectionLocks[collection] = cl
		m.mutexLock.Unlock()
	}

	return cl
}

// QueryAndUpdate - query and update documents selected by specified criteria
func (m *MemJ) QueryAndUpdate(collection string, query, payload map[string]interface{}, limit int) ([]map[string]interface{}, bool, error) {
	maxLimit := 0
	var results []map[string]interface{}
	var isUpdated bool
	var err error

	lock := m.getCollectionLock(collection)

	lock.RLock()
	defer lock.RUnlock()

	for index, value := range m.data[collection] {
		isFound, _ := m.performMatchQuery(query, value)

		if isFound {
			isUpdated, err = m.updateFields(collection, index, payload)
			if err != nil {
				// TODO: Fix partial update issue
				return results, false, err
			}
			results = append(results, value)
			if limit != 0 {
				maxLimit++
				if maxLimit >= limit {
					return results, true, nil
				}
			}
		}
	}

	return results, isUpdated, nil
}
