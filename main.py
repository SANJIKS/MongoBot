import logging
from datetime import datetime
import motor.motor_asyncio

logging.basicConfig(level=logging.INFO)

groups = {
    "hour": "%Y-%m-%dT%H:00:00",
    "day": "%Y-%m-%d",
    "month": "%Y-%m-01"
}

async def aggregate(dt_from, dt_upto, group_type):
    cluster = motor.motor_asyncio.AsyncIOMotorClient('mongodb+srv://sancho:parol123@cluster0.djrx8ct.mongodb.net/?retryWrites=true&w=majority')
    collection = cluster.testtaskdb.samplecol

    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    pipeline = [
        {
            "$match": {
                "dt": {"$gte": dt_from, "$lte": dt_upto}
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": groups[group_type],
                        "date": "$dt"
                    }
                },
                "sum": {"$sum": "$value"}
            }
        },
        {
            "$sort": {"_id": 1}
        }
    ]

    result = []
    async for doc in collection.aggregate(pipeline):
        result.append(doc)

    dataset = [data["sum"] for data in result]
    labels = [label["_id"] for label in result]
    return {"dataset": dataset, "labels": labels}
