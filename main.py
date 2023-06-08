import logging
import sys

from turbine.runtime import RecordList
from turbine.runtime import Runtime

logging.basicConfig(level=logging.INFO)


def transform(records: RecordList) -> RecordList:
    logging.info(f"processing {len(records)} record(s)")
    for record in records:
        logging.info(f"input: {record}")
        try:
            payload = record.value["payload"]["after"]
            payload["store_id"] = "002"
            payload["store_location"] = "west"

            logging.info(f"output: {record}")
        except Exception as e:
            print("Error occurred while parsing records: " + str(e))
            logging.info(f"output: {record}")
    return records


class App:
    @staticmethod
    async def run(turbine: Runtime):
        try:
            source = await turbine.resources("west-store-mongo")

            records = await source.records("medicine", {})

            # turbine.register_secrets("PWD")

            transformed = await turbine.process(records, transform)
            
            destination_db = await turbine.resources("mongo-atlas")

            await destination_db.write(transformed, "allDispensedPills", 
            # {
            #     "transforms": "unwrap",
            #     "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
            # }
            )
        except Exception as e:
            print(e, file=sys.stderr)
