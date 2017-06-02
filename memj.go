package memj

import (
	"errors"
	"strings"
	"sync"

	"github.com/google/uuid"
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
			payload["objectid"] = objectID
			m.data[collection][index] = payload
			return true, nil
		}
	}

	return false, errors.New("Not found")
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
func (m *MemJ) Query(collection string, query map[string]interface{}) ([]map[string]interface{}, error) {
	lock := m.getCollectionLock(collection)

	lock.RLock()
	defer lock.RUnlock()

	var result []map[string]interface{}
	var compareValue interface{}

	for _, value := range m.data[collection] {
		isFound := false
		for k := range query {
			key := strings.Split(k, ".")
			if len(key) == 1 {
				compareValue = value[k]
			} else {
				compareValue = m.getNestedQueryValue(key, value)
			}
			if query[k] == compareValue {
				isFound = true
			} else {
				isFound = false
				break
			}
		}

		if isFound {
			result = append(result, value)
		}
	}

	return result, nil
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
