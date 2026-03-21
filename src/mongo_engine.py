import json
import os
from pymongo import MongoClient

def loadJsonData(filePath):
    if not os.path.exists(filePath):
        return None
    with open(filePath, 'r', encoding='utf-8') as fileHandle:
        return json.load(fileHandle)

def determineMongoStrategy(fieldMetadata):
    mongoStrategyMap = {}
    for field in fieldMetadata:
        fieldName = field.get("field_name")
        isNested = field.get("is_nested", False)
        isArray = field.get("is_array", False)
        nestingDepth = field.get("nesting_depth", 0)

        if isArray and nestingDepth > 2:
            mongoStrategyMap[fieldName] = "reference"
        elif isNested and nestingDepth > 3:
            mongoStrategyMap[fieldName] = "reference"
        else:
            mongoStrategyMap[fieldName] = "embed"
            
    return mongoStrategyMap

def processMongoData(mongoData, strategyMap, dbInstance):
    mainCollection = dbInstance["main_records"]
    
    for record in mongoData:
        mainDocument = {}
        for key, value in record.items():
            strategy = strategyMap.get(key, "embed")
            
            if strategy == "reference":
                refCollection = dbInstance[key]
                insertResult = refCollection.insert_one({"data": value})
                mainDocument[key] = insertResult.inserted_id
            else:
                mainDocument[key] = value
                
        mainCollection.insert_one(mainDocument)

def runMongoEngine():
    mongoUri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    dbName = os.getenv("MONGO_DB_NAME", "cs432_db")
    
    clientInstance = MongoClient(mongoUri)
    dbInstance = clientInstance[dbName]
    
    metadataPath = os.path.join("data", "metadata.json")
    mongoDataPath = os.path.join("data", "mongo_data.json")
    
    metadataJson = loadJsonData(metadataPath)
    mongoDataJson = loadJsonData(mongoDataPath)
    
    if not metadataJson or not mongoDataJson:
        return
        
    strategyMap = determineMongoStrategy(metadataJson.get("fields", []))
    processMongoData(mongoDataJson, strategyMap, dbInstance)

if __name__ == "__main__":
    runMongoEngine()