#import neccesary modules
import os
import boto3
import csv
from word2number import w2n
import uuid
import pandas as pd
from collections import OrderedDict
from zipfile import ZipFile
##################################################################################################

# Initialize Boto3  session and Textract client 
session = boto3.Session()
bucket_name = 'invoice-extraction-files' 
region_name='ap-south-1'
textract_client = session.client(service_name='textract', region_name = region_name)
s3_client = session.client(service_name = 's3', region_name = region_name)
s3 = boto3.resource('s3')
cognito_client = boto3.client('cognito-idp', region_name = region_name)

#####################################################################################
#Get key_value pair extraction from response file   
  
def get_key_value(response):
    blocks = response['ExpenseDocuments'][0]['SummaryFields']
    dict1 ={}
    j=0
    for block in blocks:
        if 'LabelDetection' in block:
            key  = block['LabelDetection']['Text']
            value = block['ValueDetection']['Text']

            key = key.strip().lower()
            value = value.strip()
            if ("gstin" in key):
                j = j+1
                key = "GSTIN" +'_'+ str(j)
                dict1[key] = value
            dict1[key] = value

        dict2={}
        for key, value in list(dict1.items()):
            if (key != '' and dict1[key] !=''):
                dict2[key] = value

        dict3 = {}        
        try:
            for key, value  in list(dict2.items()):
                word_amount = value

                # If "amount" word detected in key then convert word amount into digit
                if 'amount' in key.lower():

                    # If given amount is in paise 
                    # split word amount in two parts (before decimal and after decimal)
                    if ' and ' in word_amount:          
                        integer =   w2n.word_to_num(word_amount.split(' and ')[0])
                        decimal =  w2n.word_to_num(word_amount.split(' and ')[-1])

                    elif ' & ' in key.lower():          # split word amount in two parts (before decimal and after decimal)
                        integer =   w2n.word_to_num(word_amount.split(' & ')[0])
                        decimal =  w2n.word_to_num(word_amount.split(' & ')[-1])

                    else:
                        integer =   w2n.word_to_num(word_amount)
                        decimal = 0.00

                    new_key = key.split('(')[0]  # change label for digit amount
                    digit  =  integer + (decimal /100)
                    dict3[new_key] = digit
                    dict3[key] = value
                else:
                    dict3[key] = value
        except:
            dict3 = dict2
    return dict3


  
############################################################################################
# get table extraction from response file
def get_table(response):
    keys = []
    dict1 = {}
    line_item = response['ExpenseDocuments'][0]['LineItemGroups'][0]['LineItems']
    for record in line_item:
        for cont in record['LineItemExpenseFields']:
            if 'LabelDetection' in cont:
                key = cont['LabelDetection']['Text']
                keys.append(key)
                
                value= cont['ValueDetection']['Text']
    
    for i in list(OrderedDict.fromkeys(keys)):
        dict1[i]=[]
    
    for record in line_item:
        row1 = record['LineItemExpenseFields']
        for cont in record['LineItemExpenseFields']:
            if 'LabelDetection' in cont: 
                key = cont['LabelDetection']['Text']
                keys.append(key)
                value= cont['ValueDetection']['Text']
                dict1[key].append(value)
    try:
        df = pd.DataFrame.from_dict(dict1)       
        return df
    except:
        print("dataframe is not supported.")


###############################################################################################
# upload invoice images into s3 bucket 
def upload_to_s3(body, bucket, key, content_type):

    upload = s3_client.put_object(Body=body, Bucket=bucket, Key=key,
                ContentType = content_type)
    return upload

###############################################################################################
# process the iploaded file with Textract
def process_text_detection(files_list, client):
    
    list_df = {}
    for file in files_list:
        response = textract_client.analyze_expense(Document={'S3Object': {
                'Bucket': bucket_name,
                'Name': file}})
        dict1 = get_key_value(response)
        dict2 = {}
        for key, value in list(dict1.items()):
            dict2[key]=[value]
        df1 = pd.DataFrame.from_dict(dict2)
        df2 = get_table(response)
        df3= pd.concat([df1, df2], axis=1)
        name = file.split('+')[-1]
        name1 = name.split('.')[0]+'.csv'
        csv = df3.to_csv(name1, index= False)
        list_df[name1] = csv
    return list_df

###############################################################################################
# convert output into csv file and upload csv file to s3
def csv_maker(list_df):

    output_filename = str(uuid.uuid4())+ '.zip'
    zip_key = 'output_csv/'+ output_filename 

    with ZipFile(output_filename, 'w') as zipObj2:
        for key, value in list(list_df.items()):
            zipObj2.write(key)
            os.remove(key)

    s3.meta.client.upload_file(Filename = output_filename, Bucket= bucket_name, Key = zip_key)
    os.remove(output_filename)
    return zip_key    
    
###############################################################################################
# send otp
def send_otp(email, otp):
    otp = str(otp)
    response = cognito_client.admin_create_user(UserPoolId='ap-south-1_WKV3d6Qhh',Username = email,
                    UserAttributes=[{'Name': 'email','Value': email},],TemporaryPassword=otp,
                        DesiredDeliveryMediums=['EMAIL'])
    return otp
    
###############################################################################################
