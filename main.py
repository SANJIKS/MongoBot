import logging
from datetime import datetime, timedelta
import motor.motor_asyncio

logging.basicConfig(level=logging.INFO)

groups = {
    "hour": "%Y-%m-%dT%H:00:00",
    "day": "%Y-%m-%dT00:00:00",
    "month": "%Y-%m-01T00:00:00"
}

#я использую облачную бд чтобы не пришлось кидать на гитхаб файл с датасетом, надеюсь так можно
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

    dates = []

    if group_type == 'hour':
        curr_date = dt_from
        while curr_date <= dt_upto:
            dates.append(curr_date)
            curr_date += timedelta(hours=1)
    
    elif group_type == 'day':
        curr_date = dt_from
        while curr_date <= dt_upto:
            dates.append(curr_date)
            curr_date += timedelta(days=1)
    
    elif group_type == 'month':
        curr_date = dt_from
        while curr_date <= dt_upto:
            dates.append(curr_date)
            if curr_date.month == 12:
                curr_date = curr_date.replace(year=curr_date.year + 1, month=1)
            else:
                curr_date = curr_date.replace(month=curr_date.month + 1)

    dataset = []
    labels = []

    for date in dates:
        format_date = date.strftime(groups[group_type])
        found = False
        
        for doc in result:
            if doc['_id'] == format_date:
                dataset.append(doc['sum'])
                labels.append(doc['_id'])
                found = True
                break
        if not found:
            dataset.append(0)
            labels.append(format_date)

    return {"dataset": dataset, "labels": labels}
