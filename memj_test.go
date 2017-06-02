package memj

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestInsert(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata"}`)

	var payload map[string]interface{}
	err := json.Unmarshal(jsonTestPayload, &payload)
	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	memj, _ := New()
	objectID, err := memj.Insert("TestCollection", payload)

	if err != nil {
		t.Error("Error inserting document: ", err)
		return
	}

	if objectID == "" {
		t.Error("Invalid objectID")
		return
	}
}

func TestFind(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata"}`)

	var payload map[string]interface{}
	err := json.Unmarshal(jsonTestPayload, &payload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	memj, _ := New()
	objectID, err := memj.Insert("TestCollection", payload)

	if err != nil {
		t.Error("Error inserting document: ", err)
		return
	}

	if objectID == "" {
		t.Error("Invalid objectID")
		return
	}

	document, err := memj.Find("TestCollection", objectID)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	returnedObjectID, ok := document["objectid"].(string)
	if !ok {
		t.Error("objectID is invalid type")
		return
	}

	if returnedObjectID != objectID {
		t.Error("Wrong object returned!")
		return
	}
}

func TestFindAll(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata"}`)

	var payload map[string]interface{}
	err := json.Unmarshal(jsonTestPayload, &payload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	memj, _ := New()
	for i := 0; i < 10; i++ {
		var objectID string
		objectID, err = memj.Insert("TestCollectionAll", payload)

		if err != nil {
			t.Error("Error inserting document: ", err)
			return
		}

		if objectID == "" {
			t.Error("Invalid objectID")
			return
		}
	}

	documents, err := memj.FindAll("TestCollectionAll")

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 10 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestUpdate(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata"}`)

	var payload map[string]interface{}
	err := json.Unmarshal(jsonTestPayload, &payload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	memj, _ := New()
	objectID, err := memj.Insert("TestCollection", payload)

	if err != nil {
		t.Error("Error inserting document: ", err)
		return
	}

	if objectID == "" {
		t.Error("Invalid objectID")
		return
	}

	var updatedTestPayload = []byte(`{"Name": "Fish", "Order": "Monotremata"}`)
	var updatedPayload map[string]interface{}
	err = json.Unmarshal(updatedTestPayload, &updatedPayload)

	if err != nil {
		t.Error("Error unmarshalling updated payload: ", err)
		return
	}

	isUpdated, err := memj.Update("TestCollection", objectID, updatedPayload)

	if err != nil {
		t.Error("Error updating: ", err)
		return
	}

	if !isUpdated {
		t.Error("Failed to update the document")
		return
	}

	document, err := memj.Find("TestCollection", objectID)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	returnedObjectID, ok := document["objectid"].(string)
	if !ok {
		t.Error("objectID is invalid type")
		return
	}

	if returnedObjectID != objectID {
		t.Error("Wrong object returned!")
		return
	}

	updatedName, ok := document["Name"].(string)
	if !ok {
		t.Error("Incorrect type of the updated field")
		return
	}

	if updatedName != "Fish" {
		t.Error("Incorrect updated field")
	}
}

func TestDelete(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata"}`)

	var payload map[string]interface{}
	err := json.Unmarshal(jsonTestPayload, &payload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	memj, _ := New()
	objectID, err := memj.Insert("TestCollection", payload)

	if err != nil {
		t.Error("Error inserting document: ", err)
		return
	}

	if objectID == "" {
		t.Error("Invalid objectID")
		return
	}

	isDeleted, err := memj.Delete("TestCollection", objectID)

	if err != nil {
		t.Error("Error deleting: ", err)
		return
	}

	if !isDeleted {
		t.Error("Failed to delete the document")
		return
	}

	_, err = memj.Find("TestCollection", objectID)

	if err == nil {
		t.Error("Error: object not deleted")
		return
	}
}

func TestQuery(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "FindMeOut", "Order": "Monotremata"}`)

	var payload map[string]interface{}
	err := json.Unmarshal(jsonTestPayload, &payload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	memj, _ := New()
	var objectID string
	objectID, err = memj.Insert("TestCollection", payload)

	if err != nil {
		t.Error("Error inserting document: ", err)
		return
	}

	if objectID == "" {
		t.Error("Invalid objectID")
		return
	}

	var jsonQuery = []byte(`{"Name": "FindMeOut"}`)
	var queryPayload map[string]interface{}
	err = json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryInManyDocumentsSingleCondition(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i, i)
		var jsonTestPayload = []byte(payloadText)

		var payload map[string]interface{}
		err := json.Unmarshal(jsonTestPayload, &payload)

		if err != nil {
			t.Error("Error unmarshalling: ", err)
			return
		}

		var objectID string
		objectID, err = memj.Insert("TestCollection", payload)

		if err != nil {
			t.Error("Error inserting document: ", err)
			return
		}

		if objectID == "" {
			t.Error("Invalid objectID")
			return
		}
	}

	var jsonQuery = []byte(`{"Name": "FindMeOut77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryInManyDocumentsWithMultipleConditions(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i, i)
		var jsonTestPayload = []byte(payloadText)

		var payload map[string]interface{}
		err := json.Unmarshal(jsonTestPayload, &payload)

		if err != nil {
			t.Error("Error unmarshalling: ", err)
			return
		}

		var objectID string
		objectID, err = memj.Insert("TestCollection", payload)

		if err != nil {
			t.Error("Error inserting document: ", err)
			return
		}

		if objectID == "" {
			t.Error("Invalid objectID")
			return
		}
	}

	var jsonQuery = []byte(`{"Name": "FindMeOut77", "Order": "Monotremata-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryInManyDocumentsSingleConditionNotFound(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i, i)
		var jsonTestPayload = []byte(payloadText)

		var payload map[string]interface{}
		err := json.Unmarshal(jsonTestPayload, &payload)

		if err != nil {
			t.Error("Error unmarshalling: ", err)
			return
		}

		var objectID string
		objectID, err = memj.Insert("TestCollection", payload)

		if err != nil {
			t.Error("Error inserting document: ", err)
			return
		}

		if objectID == "" {
			t.Error("Invalid objectID")
			return
		}
	}

	var jsonQuery = []byte(`{"Name": "FindMeOut"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload)

	if err != nil {
		t.Error("Error in query: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Got more than zero documents!")
		return
	}
}

/*func TestQueryInManyDocumentsWithMultipleConditionsNotFound(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i, i)
		var jsonTestPayload = []byte(payloadText)

		var payload map[string]interface{}
		err := json.Unmarshal(jsonTestPayload, &payload)

		if err != nil {
			t.Error("Error unmarshalling: ", err)
			return
		}

		var objectID string
		objectID, err = memj.Insert("TestCollection", payload)

		if err != nil {
			t.Error("Error inserting document: ", err)
			return
		}

		if objectID == "" {
			t.Error("Invalid objectID")
			return
		}
	}

	var jsonQuery = []byte(`{"Name": "FindMeOut77", "Order": "Monotremata-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}*/