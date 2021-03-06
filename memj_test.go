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

func TestUpdateWithAddingNewField(t *testing.T) {
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

	var updatedTestPayload = []byte(`{"Name": "Fish", "Price": 100}`)
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
		return
	}

	unchangedField, ok := document["Order"].(string)
	if !ok {
		t.Error("Incorrect type of the existing field")
		return
	}

	if unchangedField != "Monotremata" {
		t.Error("Incorrect value of the unchanged field")
		return
	}

	addedField, ok := document["Price"].(float64)
	if !ok {
		t.Error("Incorrect type of added field")
		return
	}

	if addedField != 100 {
		t.Error("Incorrect added field value")
		return
	}
}

func TestUpdateOfFieldInEmbeddedObject(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata", "Info": {"Price": 150, "Description": "Cool"}}`)

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

	var updatedTestPayload = []byte(`{"Name": "Fish", "Info.Price": 100}`)
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
		return
	}

	unchangedField, ok := document["Order"].(string)
	if !ok {
		t.Error("Incorrect type of the existing field")
		return
	}

	if unchangedField != "Monotremata" {
		t.Error("Incorrect value of the unchanged field")
		return
	}

	updatedEmbeddedObject, ok := document["Info"].(map[string]interface{})
	if !ok {
		t.Error("Incorrect type of updated embedded object")
		return
	}

	updatedEmbeddedField, ok := updatedEmbeddedObject["Price"].(float64)
	if !ok {
		t.Error("Incorrent type of updated embedded field")
		return
	}

	if updatedEmbeddedField != 100 {
		t.Error("Incorrect added field value")
		return
	}
}

func TestUpdateNotFound(t *testing.T) {
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

	isUpdated, err := memj.Update("TestCollection", objectID+"12", updatedPayload)

	if err.Error() != "Not found" {
		t.Error("Non existend object found!")
		return
	}

	if isUpdated {
		t.Error("Failed to update the document but reporting it as updated!")
		return
	}
}

func TestUpdateOfFieldInEmbeddedObjectWithWrongPath(t *testing.T) {
	var jsonTestPayload = []byte(`{"Name": "Platypus", "Order": "Monotremata", "Info": {"Price": 150, "Description": "Cool"}}`)

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

	var updatedTestPayload = []byte(`{"Name": "Fish", "Info.Price.Wrong": 100}`)
	var updatedPayload map[string]interface{}
	err = json.Unmarshal(updatedTestPayload, &updatedPayload)

	if err != nil {
		t.Error("Error unmarshalling updated payload: ", err)
		return
	}

	isUpdated, err := memj.Update("TestCollection", objectID, updatedPayload)

	if err == nil {
		t.Error("Wrong field path passed but no error returned")
		return
	}

	if isUpdated {
		t.Error("Wrong field path passed but document was updated")
		return
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

func TestDeleteNotFound(t *testing.T) {
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

	isDeleted, err := memj.Delete("TestCollection", objectID+"12")

	if err.Error() != "Not found" {
		t.Error("Object was found!")
		return
	}

	if isDeleted {
		t.Error("Not deleted object reported as deleted")
		return
	}

	_, err = memj.Find("TestCollection", objectID)

	if err != nil {
		t.Error("Error: object was deleted")
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

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

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

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryInManyDocumentsReturnSingleResult(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut", "Order": "Monotremata-%d"}`, i)
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

	documents, err := memj.Query("TestCollection", queryPayload, FindOne)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryInManyDocumentsReturnSevenResults(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut", "Order": "Monotremata-%d"}`, i)
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

	documents, err := memj.Query("TestCollection", queryPayload, 7)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 7 {
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

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

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

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in query: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Got more than zero documents!")
		return
	}
}

func TestQueryInManyDocumentsWithMultipleConditionsNotFound(t *testing.T) {
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

	var jsonQuery = []byte(`{"Name": "FindMeOut77", "Order": "Monotremata-"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Got more than zero documents!")
		return
	}
}

func TestQueryMissingKeyAndNotFound(t *testing.T) {
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

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in query: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Got more than zero documents!")
		return
	}
}

func TestQueryMultipleConditionsMissingKeyAndNotFound(t *testing.T) {
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

	var jsonQuery = []byte(`{"Name": "FindMeOut77", "Payment": "Monotremata-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Got more than zero documents!")
		return
	}
}

func TestNestedQueryInManyDocumentsSingleCondition(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i, i)
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

	var jsonQuery = []byte(`{"Order.OrderName": "NameOfOrder-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestNestedQueryInManyDocumentsWithMultipleConditions(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i, i)
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

	var jsonQuery = []byte(`{"Order.OrderID": 77, "Order.OrderName": "NameOfOrder-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestNestedQueryInManyDocumentsWithMultipleConditionsNotFound(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i, i)
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

	var jsonQuery = []byte(`{"Order.OrderIDs": 77, "Order.OrderName": "NameOfOrder-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestNestedQueryInManyDocumentsWithMultipleConditionsNotExist(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i, i)
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

	var jsonQuery = []byte(`{"Order.OrderID": 77, "Order.OrderName.x": "NameOfOrder-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestNestedQueryInManyDocumentsWithMultipleConditionsMissingKey(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i, i)
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

	var jsonQuery = []byte(`{"Order.OrderID": 77, "": "NameOfOrder-77"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestNestedQueryReturnManyDocuments(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i%10, i)
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

	var jsonQuery = []byte(`{"Order.OrderID": 7}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 10 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestOrQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": %d, "OrderName": "NameOfOrder-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"$or": [{"OrderID": 7}, {"OrderName": "NameOfOrder-7"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 10 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestAndQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": %d, "OrderName": "NameOfOrder-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"$and": [{"OrderID": 7}, {"OrderName": "NameOfOrder-7"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestInvalidLogicalQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": %d, "OrderName": "NameOfOrder-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"$ander": [{"OrderID": 7}, {"OrderName": "NameOfOrder-7"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestEmptyLogicalSubQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": %d, "OrderName": "NameOfOrder-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"$and": [{}, {"OrderName": "NameOfOrder-7"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestEmptyLogicalQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": %d, "OrderName": "NameOfOrder-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"$and": []}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 100 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestLogicalQueryNotList(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": %d, "OrderName": "NameOfOrder-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"$and": "blah"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err.Error() != "Logical operator query has invalid syntax.  Expected a list of queries." {
		t.Error("Error should be reported about invalid query syntax")
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestLogicalNestedAndQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i%10, i)
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

	var jsonQuery = []byte(`{"$and": [{"Order.OrderID": 7}, {"Order.OrderName": "NameOfOrder-7"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in query: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestLogicalNestedOrQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i%10, i)
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

	var jsonQuery = []byte(`{"$or": [{"Order.OrderID": 7}, {"Order.OrderName": "NameOfOrder-7"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in query: ", err)
		return
	}

	if len(documents) != 10 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestLogicalNestedOrAndQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i%10, i)
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

	var jsonQuery = []byte(`{"$or": [{"$and": [{"Order.OrderID": 7}, {"Order.OrderName": "NameOfOrder-7"}]}, {"$and": [{"Order.OrderID": 9}, {"Order.OrderName": "NameOfOrder-9"}]}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in query: ", err)
		return
	}

	if len(documents) != 2 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestLogicalInvalidNestedOrAndQuery(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": {"OrderID": %d, "OrderName": "NameOfOrder-%d"}}`, i, i%10, i)
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

	var jsonQuery = []byte(`{"$or": [{"$and": [{"Order.OrderID": 7}, {"Order.OrderName": "NameOfOrder-7"}]}, {"$and": {"Order.OrderID": 9}}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err.Error() != "Logical operator query has invalid syntax.  Expected a list of queries." {
		t.Error("Error should be reported about invalid query syntax")
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryAndUpdate(t *testing.T) {
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

	var jsonUpdate = []byte(`{"Order": "Fish-77"}`)
	var queryUpdatePayload map[string]interface{}
	err = json.Unmarshal(jsonUpdate, &queryUpdatePayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	_, isUpdated, err := memj.QueryAndUpdate("TestCollection", queryPayload, queryUpdatePayload, NoLimit)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if !isUpdated {
		t.Error("Incorrect number of documents returned")
		return
	}

	var jsonQuery2 = []byte(`{"Order": "Fish-77"}`)
	var queryPayload2 map[string]interface{}
	err = json.Unmarshal(jsonQuery2, &queryPayload2)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload2, NoLimit)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryAndUpdateManyDocsUpdated(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"Name": "FindMeOut7"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	var jsonUpdate = []byte(`{"Order": "Fish-7"}`)
	var queryUpdatePayload map[string]interface{}
	err = json.Unmarshal(jsonUpdate, &queryUpdatePayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	_, isUpdated, err := memj.QueryAndUpdate("TestCollection", queryPayload, queryUpdatePayload, NoLimit)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if !isUpdated {
		t.Error("Incorrect number of documents returned")
		return
	}

	var jsonQuery2 = []byte(`{"Order": "Fish-7"}`)
	var queryPayload2 map[string]interface{}
	err = json.Unmarshal(jsonQuery2, &queryPayload2)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload2, NoLimit)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if len(documents) != 10 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryAndUpdateManyDocsUpdatedLimitToOne(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"Name": "FindMeOut7"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	var jsonUpdate = []byte(`{"Order": "Fish-7"}`)
	var queryUpdatePayload map[string]interface{}
	err = json.Unmarshal(jsonUpdate, &queryUpdatePayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	_, isUpdated, err := memj.QueryAndUpdate("TestCollection", queryPayload, queryUpdatePayload, FindOne)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if !isUpdated {
		t.Error("Incorrect number of documents returned")
		return
	}

	var jsonQuery2 = []byte(`{"Order": "Fish-7"}`)
	var queryPayload2 map[string]interface{}
	err = json.Unmarshal(jsonQuery2, &queryPayload2)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload2, NoLimit)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryAndUpdateManyDocsUpdatedLimitToSix(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"Name": "FindMeOut%d", "Order": "Monotremata-%d"}`, i%10, i)
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

	var jsonQuery = []byte(`{"Name": "FindMeOut7"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	var jsonUpdate = []byte(`{"Order": "Fish-7"}`)
	var queryUpdatePayload map[string]interface{}
	err = json.Unmarshal(jsonUpdate, &queryUpdatePayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	_, isUpdated, err := memj.QueryAndUpdate("TestCollection", queryPayload, queryUpdatePayload, 6)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if !isUpdated {
		t.Error("Incorrect number of documents returned")
		return
	}

	var jsonQuery2 = []byte(`{"Order": "Fish-7"}`)
	var queryPayload2 map[string]interface{}
	err = json.Unmarshal(jsonQuery2, &queryPayload2)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload2, NoLimit)

	if err != nil {
		t.Error("Error: ", err)
		return
	}

	if len(documents) != 6 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestQueryAndUpdateWithInvalidUpdate(t *testing.T) {
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

	var jsonQuery = []byte(`{"Name": "FindMeOut7"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	var jsonUpdate = []byte(`{"bore.oir": []}`)
	var queryUpdatePayload map[string]interface{}
	err = json.Unmarshal(jsonUpdate, &queryUpdatePayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	_, isUpdated, err := memj.QueryAndUpdate("TestCollection", queryPayload, queryUpdatePayload, NoLimit)

	if err == nil {
		t.Error("Invalid query syntax but no error returned")
		return
	}

	if isUpdated {
		t.Error("Update done even though query syntax was wrong")
		return
	}
}

// Comparison operators - float64
func TestComparisonGT(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gt": 50}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 49 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonGTE(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gte": 50}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 50 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonLT(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$lt": 50}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 50 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonLTE(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$lte": 50}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 51 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonEQ(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$eq": 50}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonNE(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$ne": 50}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 99 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

// Comparison operators - strings
func TestComparisonGTStr(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": "%d"}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gt": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 53 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonGTEStr(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": "%d"}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gte": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 54 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonLTStr(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": "%d"}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$lt": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 46 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonLTEStr(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": "%d"}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$lte": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 47 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonEQStr(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": "%d"}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$eq": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonNEStr(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": "%d"}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$ne": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 99 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

// Invalid comparisons
func TestComparisonInvalidType(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gt": "50"}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err == nil {
		t.Error("Incorrect types in comparison but no error")
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonUnsupportedType(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gt": [23, 23]}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err == nil {
		t.Error("Incorrect types in comparison but no error")
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonUnsupportedComplexType(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gt": {}}}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err == nil {
		t.Error("Incorrect types in comparison but no error")
		return
	}

	if len(documents) != 0 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonWithLogicalAnd(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"$and": [{"OrderPrice": {"$gte": 50}}, {"OrderID": "id-57"}]}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}

func TestComparisonWithSelection(t *testing.T) {
	memj, _ := New()

	for i := 0; i < 100; i++ {
		payloadText := fmt.Sprintf(`{"OrderID": "id-%d", "OrderPrice": %d}`, i, i)
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

	var jsonQuery = []byte(`{"OrderPrice": {"$gte": 50}, "OrderID": "id-57"}`)
	var queryPayload map[string]interface{}
	err := json.Unmarshal(jsonQuery, &queryPayload)

	if err != nil {
		t.Error("Error unmarshalling: ", err)
		return
	}

	documents, err := memj.Query("TestCollection", queryPayload, NoLimit)

	if err != nil {
		t.Error("Error in Find: ", err)
		return
	}

	fmt.Println(len(documents))
	if len(documents) != 1 {
		t.Error("Incorrect number of documents returned")
		return
	}
}
