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
                # FIX: use underscores instead of dots in collection names
                collectionName = fieldPath.replace(".", "_")
                refCollection = dbInstance[collectionName]
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
    success_count = 0
    fail_count = 0

    for record in mongoData:
        try:
            extractedRecordId = record.pop("record_id", None)
            processedRecord = processNode(record, "", dbInstance, strategyMap)

            if extractedRecordId is not None:
                processedRecord["_id"] = extractedRecordId

            mainCollection.insert_one(processedRecord)
            success_count += 1
        except Exception as e:
            fail_count += 1
            print(f"[!] Failed to insert Mongo record: {e}")

    return success_count, fail_count


def runMongoEngine():
    # FIX: use config constants instead of hardcoded relative paths
    from src.config import MONGO_URI, MONGO_DB_NAME, METADATA_FILE, MONGO_DATA_FILE

    print("\n" + "=" * 80)
    print("MONGO PIPELINE ORCHESTRATOR")
    print("=" * 80)

    clientInstance = MongoClient(MONGO_URI)
    dbInstance = clientInstance[MONGO_DB_NAME]

    metadataJson = loadJsonData(METADATA_FILE)
    mongoDataJson = loadJsonData(MONGO_DATA_FILE)

    if not metadataJson:
        print(f"[!] Metadata not found at {METADATA_FILE}. Run initialise first.")
        return

    if not mongoDataJson:
        print(f"[!] Mongo data not found at {MONGO_DATA_FILE}. Run routing first.")
        return

    if len(mongoDataJson) == 0:
        print("[*] mongo_data.json is empty — nothing to insert.")
        return

    print(f"[*] Loading {len(mongoDataJson)} records into MongoDB...")

    strategyMap = determineMongoStrategy(metadataJson.get("fields", []))
    success_count, fail_count = processMongoData(mongoDataJson, strategyMap, dbInstance)

    print("\n" + "=" * 80)
    print("MONGO PIPELINE SUMMARY")
    print("=" * 80)
    print(f"\nDatabase    : {MONGO_DB_NAME}")
    print(f"\nLoad Results:")
    print(f"  Successful Inserts : {success_count}")
    print(f"  Failed Inserts     : {fail_count}")
    print(f"  Total Processed    : {success_count + fail_count}")

    print(f"\nCollections in database:")
    for col in dbInstance.list_collection_names():
        count = dbInstance[col].count_documents({})
        print(f"  {col:<35} {count:>10} documents")

    print("=" * 80)


if __name__ == "__main__":
    runMongoEngine()