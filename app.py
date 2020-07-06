import boto3
import requests
from flask import Flask, request
from boto3.dynamodb.conditions import Key


from flask import Flask, render_template

app = Flask(__name__)

dynamodb = boto3.resource('dynamodb', region_name="ap-southeast-1")
table=dynamodb.Table("dturesults")


@app.route('/', methods=['GET', 'POST'])
def index():
    errors = []
    results = {}
    student_name = None
    rollno = None
    if request.method == "POST":
        # get rollno that the user has entered
        try:
            rollno = request.form['rollno']
            response = table.query(
                KeyConditionExpression=Key('rollno').eq(rollno)
            )
            item = response["Items"]
            if not item:
                errors.append("No record found for rollno={}".format(rollno))
            else:
                results = response["Items"][0]
                student_name = results["name"]
                del results["name"]
                del results["name"]
        except Exception as err:
            errors.append(repr(err))
    return render_template('index.html', errors=errors, results=results, student_name=student_name, rollno=rollno)


if __name__ == '__main__':
    app.run()