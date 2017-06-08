# MemJ
MemJ is simple non-sql like, JSON based in memory storage package.  It is meant
primarily for unit testing code that needs to store/query JSON documents.

# Queries
Queries are in JSON in following format (based on mongodb):

```
{"key": "value"} - simple query to find all documents that match value for the key
{"key1": "value1", "key2": "value2"} - matches all documents where key1 and key2 match their values
{"$or": [{"key1": "value1"}, {"key2": "value2"}]} - match all documents where key1 equals its value
        or key2 equals its value.
{"$and": [{"key1": "value1"}, {"key2": "value2"}]} - match all documents where key1 equals its value
        and key2 equals its value.
```

You can also query fields within objects of other fields.  For example, following document:
```
{"Name": "FindMeOut7", "Order": {"OrderID": 7, "OrderName": "NameOfOrder-7"}}
```

Could be queried like this:
```
{"Order.OrderID": 7, "Order.OrderName": "OrderNameOfOrder-7"}
```

You can also nest queries like this:
```
{"$or": [
    {"$and": [{"Order.OrderID": 7}, {"Order.OrderName": "NameOfOrder-7"}]},
    {"$and": [{"Order.OrderID": 9}, {"Order.OrderName": "NameOfOrder-9"}]}
  ]
}
```
