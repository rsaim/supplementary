from pymongo import MongoClient
import boto3

client = MongoClient(host="localhost", port=27017)
db = client.dtu
results=db.get_collection("results")
all_results = list(results.find())

dynamodb = boto3.resource('dynamodb', region_name="ap-southeast-1")
table=dynamodb.Table("dturesults")
with table.batch_writer() as batch:
    for row in all_results:
        row["rollno"] = row["_id"]
        row = {str(k):str(v) for k,v in row.items()}
        del row["_id"]
        try:
            batch.put_item(Item=row)
        except Exception as err:
            print(f"Error while inserting {row!r}")
            raise err
