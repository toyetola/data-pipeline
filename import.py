import os
import json
from datetime import datetime
from google.cloud import storage

day = datetime.now().day
month = datetime.now().month
year = datetime.now().year
hour = datetime.now().hour


#this is gotten from environment ensure you set GOOGLE_APPLICATION_CREDENTIALS=my_json.json as environment variable
#we could include the credentials details here but it not best practice
client = storage.Client()
bucket = client.get_bucket('acc-imports')


def uploadFile(bucket):
    f = input('File path:')
    filename = os.path.basename(f)
    destination_filepath = "import"+"/"+str(year)+"/"+str(month)+"/"+str(day)+"/"+str(hour)+"/"+filename
    blob = bucket.blob(destination_filepath)
    blob.upload_from_filename(f)
    blob.make_public()
    url = blob.public_url
    print(
            "File {} uploaded to {}".format(
                filename, destination_filepath
            )
    )

uploadFile(bucket)
