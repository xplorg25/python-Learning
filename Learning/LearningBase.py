import sys
import time
from collections import defaultdict
from datetime import timedelta, datetime
import pymongo as mongo
import logging
import os
from logging.handlers import RotatingFileHandler
import DB_details as dbd

def setup_logging(log_file="data_processing.log"):
    """Configure logging to write to both file and console"""
    # Create logs directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_path = os.path.join(log_dir, log_file)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create formatters
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # File handler with rotation (10 MB per file, keep 5 backup files)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,  # 10 MB per file
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger





def process_day(date_str, collection, collection2, to_save_coll, logger,tc):
    if not date_str:
        return 0

    logger.info(f"Processing day: {date_str}")
    start_time = time.perf_counter()
    inserted_count = 0

    try:
        # Use projection to limit fields and add index hint if available
        # cursor = collection.find(
        #     {"hwNmNorthboundEventDate": date_str, "hwNmNorthboundFaultFlag": {"$ne": "Recovery"} ,"snmpTrapOID" : {"$ne":"hwNmNorthboundEventKeepAlive"}},
        #     {"hwNmNorthboundEventDate": 1, "hwNmNorthboundEventTime": 1, "hwNmNorthboundEventName": 1,
        #      "hwNmNorthboundFaultFlag": 1, "hwNmNorthboundNEName": 1, "hwNmNorthboundObjectInstance": 1},
        #     no_cursor_timeout=True,
        #     batch_size=1000  # Optimize batch size
        # )
        cursor = collection.find(
            {"hwNmNorthboundEventDate": date_str,
             "snmpTrapOID": {"$ne": "hwNmNorthboundEventKeepAlive"}},
            {"hwNmNorthboundEventDate": 1, "hwNmNorthboundEventTime": 1, "hwNmNorthboundEventName": 1,
             "hwNmNorthboundFaultFlag": 1, "hwNmNorthboundNEName": 1, "hwNmNorthboundObjectInstance": 1},
            no_cursor_timeout=True,
            batch_size=1000  # Optimize batch size
        )

        # Prepare bulk operations
        bulk_operations = []
        batch_size = 500

        time_below = datetime.strptime("00:00:29", "%H:%M:%S").time()
        time_above = datetime.strptime("23:59:30", "%H:%M:%S").time()

        doc_count = 0
        for h_doc in cursor:
            rec = False
            doc_count += 1
            if doc_count % 10000 == 0:
                logger.info(f"Processed {doc_count} documents for date {date_str}")

            if h_doc["hwNmNorthboundFaultFlag"] == "Fault" or h_doc["hwNmNorthboundFaultFlag"] == "Recovery":
                if h_doc["hwNmNorthboundFaultFlag"] == "Recovery":
                    rec = True
                neName = h_doc["hwNmNorthboundNEName"]
                managedObj = h_doc["hwNmNorthboundObjectInstance"]
                alarmName = h_doc["hwNmNorthboundEventName"]
                time_str = h_doc["hwNmNorthboundEventTime"]
                date_str = h_doc["hwNmNorthboundEventDate"]
                key_h = f"{neName}**{managedObj}**{alarmName}"

                if rec:
                    key_h+="_rec"

                tc[key_h]+=1

                time_obj = datetime.strptime(time_str, "%H:%M:%S")
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")

                # Calculate time window efficiently
                filtered_time2 = (time_obj + timedelta(seconds=30)).strftime("%H:%M:%S")
                filtered_time1 = (time_obj - timedelta(seconds=30)).strftime("%H:%M:%S")

                # Determine date range
                filtered_date1 = date_obj
                filtered_date2 = date_obj

                if time_obj.time() <= time_below:
                    filtered_date1 = date_obj - timedelta(days=1)
                elif time_obj.time() >= time_above:
                    filtered_date2 = date_obj + timedelta(days=1)

                # Format dates once
                date1_str = filtered_date1.strftime('%Y-%m-%d')
                date2_str = filtered_date2.strftime('%Y-%m-%d')

                # Optimize query with proper indexing
                query = {
                    "firstTimeDetectedDate": {"$gte": date1_str, "$lte": date2_str},
                    "firstTimeDetectedTime": {"$gte": filtered_time1, "$lte": filtered_time2},
                    "severity": {"$nin": ["Warning", "Cleared"]}
                }

                if rec:
                    query["severity"] = "Cleared"

                # Use projection and proper indexing
                sam_cursor = collection2.find(
                    query,
                    {"firstTimeDetectedDate": 1, "firstTimeDetectedTime": 1, "alarmName": 1,
                     "objectFullName": 1, "nodeName": 1}
                )

                inner_dict = defaultdict(int)
                for res in sam_cursor:
                    #key2 = f"{res['nodeName'].replace('.', r'\u002e')}**{res['objectFullName'].replace('.', r'\u002e')}**{res['alarmName'].replace('.', r'\u002e')}"
                    key2 = str(res["nodeName"].replace('.', r'\u002e') +
                               "**" + res["objectFullName"].replace('.', r'\u002e') +
                               "**" + res["alarmName"].replace(".", r"\u002e"))
                    inner_dict[key2] += 1

                # Skip empty results
                if not inner_dict:
                    continue

                # Prepare update operation
                update_query = {"key1": key_h}
                update_values = {
                    "$inc": {f"nested_count.{k}": v for k, v in inner_dict.items()}
                }
                update_values["$inc"]["count"] = 1

                bulk_operations.append(
                    mongo.UpdateOne(update_query, update_values, upsert=True)
                )

                # Execute bulk operations in batches
                if len(bulk_operations) >= batch_size:
                    if bulk_operations:
                        result = to_save_coll.bulk_write(bulk_operations)
                        inserted_count += result.upserted_count
                        logger.info(
                            f"Bulk write completed. Upserted: {result.upserted_count}, Modified: {result.modified_count}")
                        bulk_operations = []

        # Execute remaining operations
        if bulk_operations:
            result = to_save_coll.bulk_write(bulk_operations)
            inserted_count += result.upserted_count
            logger.info(
                f"Final bulk write completed. Upserted: {result.upserted_count}, Modified: {result.modified_count}")

        cursor.close()

        end_time = time.perf_counter()
        processing_time = (end_time - start_time) / 60
        logger.info(
            f"Day {date_str} completed in {processing_time:.2f} minutes. Total documents processed: {doc_count}, Inserted: {inserted_count}")

        return inserted_count

    except Exception as e:
        logger.error(f"Error processing day {date_str}: {str(e)}", exc_info=True)
        return 0


def ensure_indexes(collections, logger):
    """Create required indexes if they don't exist"""
    try:
        logger.info("Checking and creating required indexes...")

        # Collection indexes
        if "hwNmNorthboundEventDate_1_hwNmNorthboundFaultFlag_1" not in collections["huawei_mpbn_alarms"].index_information():
            logger.info("Creating index on hia collection...")
            collections["huawei_mpbn_alarms"].create_index([
                ("hwNmNorthboundEventDate", 1),
                ("hwNmNorthboundFaultFlag", 1)
            ])
            logger.info("Index created successfully on hua collection")

        # Collection2 indexes
        compound_index_name = "firstTimeDetectedDate_1_firstTimeDetectedTime_1_severity_1"
        if compound_index_name not in collections["sam_ipran_alarms"].index_information():
            logger.info("Creating index on sma collection...")
            collections["sam_ipran_alarms"].create_index([
                ("firstTimeDetectedDate", 1),
                ("firstTimeDetectedTime", 1),
                ("severity", 1)
            ])
            logger.info("Index created successfully on sam collection")

        # Output collection index
        if "hashed_key1" not in collections["congoTesting_corels_without_clear"].index_information():
            logger.info("Creating hashed index on test_ collection...")
            collections["congoTesting_corels_without_clear"].create_index([("key1", "hashed")], name="hashed_key1")
            logger.info("Hashed index created successfully on saving col collection")

        logger.info("All required indexes are in place")
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}", exc_info=True)
        raise


def main():
    # Setup logging
    logger = setup_logging("mongodb_processing.log")
    logger.info("Starting data processing job")
    tc= defaultdict(int)
    try:
        # Record start time for overall processing
        overall_start = time.perf_counter()

        connection = dbd.getConnection()
        logger.info("MongoDB connection established")

        db = connection[dbd.dataBase]
        collection = db[dbd.from_db]
        collection2 = db[dbd.to_db]

        to_save = connection[dbd.to_save_db]
        to_save_coll = to_save[dbd.to_save_collection]

        # Ensure all needed indexes exist
        ensure_indexes({
            dbd.from_db : collection,
            dbd.to_db : collection2,
            dbd.to_save_collection : to_save_coll
        }, logger)

        # Get distinct dates - use a cursor to avoid loading all into memory
        logger.info("Retrieving distinct dates for processing")
        distinct_dates = sorted([d for d in collection.distinct("hwNmNorthboundEventDate") if d])
        logger.info(f"Found {len(distinct_dates)} dates to process")

        total_processed = 0

        # Option 1: Process in batches sequentially
        for i, date_str in enumerate(distinct_dates):
            logger.info(f"Processing date {i + 1}/{len(distinct_dates)}: {date_str}")
            count = process_day(date_str, collection, collection2, to_save_coll, logger,tc)
            total_processed += count
            logger.info(
                f"Progress: {i + 1}/{len(distinct_dates)} dates processed ({((i + 1) / len(distinct_dates)) * 100:.1f}%)")

        # Option 2: Use thread pool for parallel processing (uncomment to use)
        # Adjust max_workers based on your system capabilities
        """
        logger.info("Starting parallel processing with ThreadPoolExecutor")
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_day, date_str, collection, collection2, to_save_coll, logger)
                for date_str in distinct_dates if date_str
            ]

            total_processed = 0
            for i, future in enumerate(futures):
                try:
                    count = future.result()
                    total_processed += count
                    logger.info(f"Progress: {i+1}/{len(futures)} dates processed ({((i+1)/len(futures))*100:.1f}%)")
                except Exception as e:
                    logger.error(f"Error in thread execution: {str(e)}", exc_info=True)
        """

        overall_end = time.perf_counter()
        total_time = (overall_end - overall_start) / 60

        logger.info(f"All processing completed. Total time: {total_time:.2f} minutes")
        logger.info(f"Total documents processed: {total_processed}")
        print(tc)

    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}", exc_info=True)
    finally:
        logger.info("Data processing job ended")


if __name__ == "__main__":
    main()