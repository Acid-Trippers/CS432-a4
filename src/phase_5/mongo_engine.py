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
                # Use underscores instead of dots in collection names
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

    for idx, record in enumerate(mongoData):
        try:
            extractedRecordId = record.pop("record_id", None)
            processedRecord = processNode(record, "", dbInstance, strategyMap)

            if extractedRecordId is not None:
                processedRecord["_id"] = extractedRecordId

            # FIX: use upsert instead of insert_one to handle fetch runs gracefully.
            # On initialise — all records are new, upsert inserts them.
            # On fetch — new records get inserted, existing ones get updated in place.
            # This prevents E11000 duplicate key errors when mongo_data.json contains
            # cumulative records from previous runs.
            mainCollection.update_one(
                {"_id": processedRecord["_id"]},
                {"$set": processedRecord},
                upsert=True
            )
            success_count += 1
            
            # Progress indicator every 100 records
            if (idx + 1) % 100 == 0:
                print(f"[*] Processed {idx + 1}/{len(mongoData)} records...", flush=True)
                
        except Exception as e:
            fail_count += 1
            print(f"[!] Failed to upsert Mongo record: {e}", flush=True)

    return success_count, fail_count


def runMongoEngine():
    from src.config import MONGO_URI, MONGO_DB_NAME, METADATA_FILE, MONGO_DATA_FILE

    print("\n" + "=" * 80, flush=True)
    print("MONGO PIPELINE ORCHESTRATOR", flush=True)
    print("=" * 80, flush=True)

    clientInstance = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    dbInstance = clientInstance[MONGO_DB_NAME]

    metadataJson = loadJsonData(METADATA_FILE)
    mongoDataJson = loadJsonData(MONGO_DATA_FILE)

    if not metadataJson:
        print(f"[!] Metadata not found at {METADATA_FILE}. Run initialise first.", flush=True)
        clientInstance.close()
        return 0, 1

    if not mongoDataJson:
        print(f"[!] Mongo data not found at {MONGO_DATA_FILE}. Run routing first.", flush=True)
        clientInstance.close()
        return 0, 1

    if len(mongoDataJson) == 0:
        print("[*] mongo_data.json is empty — nothing to insert.", flush=True)
        clientInstance.close()
        return 0, 0

    print(f"[*] Loading {len(mongoDataJson)} records into MongoDB...", flush=True)

    strategyMap = determineMongoStrategy(metadataJson.get("fields", []))
    success_count, fail_count = processMongoData(mongoDataJson, strategyMap, dbInstance)

    print("\n" + "=" * 80, flush=True)
    print("MONGO PIPELINE SUMMARY", flush=True)
    print("=" * 80, flush=True)
    print(f"\nDatabase    : {MONGO_DB_NAME}", flush=True)
    print(f"\nLoad Results:", flush=True)
    print(f"  Successful Upserts : {success_count}", flush=True)
    print(f"  Failed Upserts     : {fail_count}", flush=True)
    print(f"  Total Processed    : {success_count + fail_count}", flush=True)

    print(f"\nCollections in database:", flush=True)
    for col in dbInstance.list_collection_names():
        count = dbInstance[col].count_documents({})
        print(f"  {col:<35} {count:>10} documents", flush=True)

    if fail_count == 0:
        # Batch payload is fully consumed; clear so next fetch processes only new records.
        try:
            with open(MONGO_DATA_FILE, 'w', encoding='utf-8') as fileHandle:
                json.dump([], fileHandle)
        except Exception as e:
            print(f"[!] Warning: could not clear {MONGO_DATA_FILE}: {e}", flush=True)

    print("=" * 80, flush=True)

    clientInstance.close()
    return success_count, fail_count


if __name__ == "__main__":
    runMongoEngine()