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

        if isArray or isNested:
            mongoStrategyMap[fieldName] = "reference"
        else:
            mongoStrategyMap[fieldName] = "embed"
            
    return mongoStrategyMap

def processNode(dataNode, currentPath, dbInstance, strategyMap):
    if isinstance(dataNode, dict):
        processedDict = {}
        for key, value in dataNode.items():
            fieldPath = f"{currentPath}.{key}" if currentPath else key
            strategy = strategyMap.get(fieldPath, "embed")
            
            processedValue = processNode(value, fieldPath, dbInstance, strategyMap)
            
            if strategy == "reference":
                refCollection = dbInstance[fieldPath]
                insertResult = refCollection.insert_one({"data": processedValue})
                processedDict[key] = insertResult.inserted_id
            else:
                processedDict[key] = processedValue
        return processedDict
        
    elif isinstance(dataNode, list):
        processedList = []
        for item in dataNode:
            elementPath = f"{currentPath}[]"
            processedItem = processNode(item, elementPath, dbInstance, strategyMap)
            processedList.append(processedItem)
        return processedList
        
    else:
        return dataNode

def processMongoData(mongoData, strategyMap, dbInstance):
    mainCollection = dbInstance["main_records"]
    
    for record in mongoData:
        extractedRecordId = record.pop("record_id", None)
        processedRecord = processNode(record, "", dbInstance, strategyMap)
        
        if extractedRecordId is not None:
            processedRecord["_id"] = extractedRecordId
            
        mainCollection.insert_one(processedRecord)

def runMongoEngine():
    from src.config import MONGO_URI, MONGO_DB_NAME
    
    clientInstance = MongoClient(MONGO_URI)
    dbInstance = clientInstance[MONGO_DB_NAME]
    
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