import json
import os
from google.cloud import storage
from datetime import datetime

partnerList = ["Partner_A", "Partner_B", "Partner_C", "Partner_D", "Partner_E",
               "Partner_F", "Partner_G", "Partner_H", "Partner_I", "Partner_J",
               "Partner_K", "Partner_L", "Partner_M", "Partner_N", "Partner_O"]
holdTempData = []
finalList = []

documentPath = input("Please provide the url to your input file: ")

#The top level componet function that runs the pipeline
def consolidate(documentPath, partnerList):
    global firstParsing
    with open(documentPath) as f:
        filename = os.path.basename(f.name)
        data = json.load(f)
        for obj in data:
            #print(obj)     
            if 'accommodation_data' in obj and 'accommodation_id' in obj:
                firstParsing = compareWithPartnerList(obj, partnerList)
            else:
                print('error: the needed consilidation data does not exist.')
    #print(firstParsing)
    cleanData = getFinalisedData(firstParsing)
    print(cleanData)
    outputToFile(cleanData)

#determine the positions of priority of partner list and label accordingly  and put in a temporary memory
    
def compareWithPartnerList(obj, partnerlist):
    if obj['partner_name'] in partnerList:
        obj['position'] = partnerList.index(obj['partner_name'])
        holdTempData.append(obj)
        return holdTempData
    else :
        print('Partner does not exist.')


#seive through the data genrated from the above step determine if they have same accomodation_ids and get the priority
#algorithm to play out
#returns the actual needed data
        
def getFinalisedData(arr):
    for ar in arr:
        if len(finalList) > 0:
            for x in finalList:
                if x['accommodation_id'] == ar['accommodation_id'] :
                    if partnerList.index(x['partner_name']) < ar['position']:
                        pass
                    else:
                        finalList.remove(x)
                        if 'accommodation_data' in ar:
                            ar['accommodation_data'] = {'accommodation_name':ar['accommodation_data']['accommodation_name']}
                        finalList.append(ar)                   
                elif  x['accommodation_id'] != ar['accommodation_id']:
                    ar.pop('position')
                    if 'accommodation_data' in ar:
                        ar['accommodation_data'] = {'accommodation_name':ar['accommodation_data']['accommodation_name']}
                    finalList.append(ar)
        else:
            ar.pop('position')
            if 'accommodation_data' in ar:
                ar['accommodation_data'] = {'accommodation_name':ar['accommodation_data']['accommodation_name']}
            finalList.append(ar)
    for itm in finalList:
        itm.pop('partner_name')
    return finalList
       

#output to file

def outputToFile(consolidatedData):
    client = storage.Client()
    bucket_name = 'trivago-output'
    bucket_info = client.lookup_bucket(bucket_name) 
     
    if not bucket_info :
        bucket = client.create_bucket(bucket_name)
    else:
        bucket = client.get_bucket(bucket_name)
    with open('output.json', 'w', encoding='utf-8') as f:
        json.dump(consolidatedData, f, ensure_ascii=False, indent=4)
        #filename = os.path.basename(f)
        day = datetime.now().day
        month = datetime.now().month
        year = datetime.now().year
        hour = datetime.now().hour
        destination_filepath = "outputfiles"+"/"+str(year)+"/"+str(month)+"/"+str(day)+"/"+str(hour)+"/"+f.name
        blob = bucket.blob(destination_filepath)
        blob.upload_from_filename(f.name)
        blob.make_public()
        url = blob.public_url
        print(
                "File {} uploaded to {}".format(
                    f.name, destination_filepath
                )
        )

consolidate(documentPath, partnerList)




