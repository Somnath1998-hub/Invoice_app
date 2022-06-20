#import neccesary modules
import os
import boto3
import csv
from word2number import w2n
import uuid
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
  
def get_key_value_pairs(response):
    dict1={}
    blocks = response['ExpenseDocuments'][0]['SummaryFields']
    
    for block in blocks:
        if 'LabelDetection' in block:
            key  = block['LabelDetection']['Text']
            value = block['ValueDetection']['Text']
            dict1[key.strip()] = value.strip()      # strip extra white spaces form key and value
        else:
            key  = block['Type']['Text']
            value = block['ValueDetection']['Text']
            dict1[key.strip()] = value.strip()
    return dict1
  
############################################################################################
# get table extraction from response file
def get_table(response):
    key_value = {}
    j = 0
    line_item = response['ExpenseDocuments'][0]['LineItemGroups'][0]['LineItems']
    for record in line_item:
        j+=1
        row1 = record['LineItemExpenseFields']
        for cont in record['LineItemExpenseFields']:
            if 'LabelDetection' in cont: 

                # Add coustom keys by changing detected keys to avoid overlapping in dictonary 
                key = cont['LabelDetection']['Text']+'_'+str(j)
                value= cont['ValueDetection']['Text']
                key_value[key]=value
            
    return key_value

#############################################################################################
# convert word amount into digit

def word_to_digit(dict3):
    dict1 = get_gst_key(dict3)
    dict2 = {}
    try:
        for key, value  in list(dict1.items()):
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
                dict2[new_key] = digit
                dict2[key] = value
            else:
                dict2[key] = value
    except:
        dict2 = dict1 
    return dict2

###############################################################################################
# change keys to avoid multiple column for same value

def get_gst_key(dict1):
    dict2 = {}
    j = 0
    string = ""
    try:
        for key, value in list(dict1.items()):
                if ("GSTIN" in key) or ("GST" in key):   # add GSTIN/GST/GSTIN.etc in one key
                    j = j+1
                    gst_key = "GSTIN" +'_'+ str(j)
                    gst_value = value
                    dict2[gst_key] = gst_value
                    del dict1[key]

                else:
                    if (key == ""):                 # replace empty key with 'other'
                        key = "other"
                        value = string + ", "+ str(value)
                        dict2[key] = value
                    else:
                        key = key.lower()       # convert upper case/ sentance case keys to lower case
                        key = key.replace(".","") # remove '.' from keys
                        key = key.replace(":","").strip() # remove ':' from key
                        dict2[key] = value
    except:
        dict2 = dict1 
    return dict2

###################################################################################################
# If "Descrption" in key then change key to "description_of_goods" + '_' + str(j) to avoid multiple cloumns

def key_correction(key_value):      
    dict2 = {}
    j = 0
    try:
        for key, value in list(key_value.items()):
                if 'Description' in key:
                    j = j + 1
                    goods_key = "description_of_goods" + '_' + str(j)
                    dict2[goods_key] = value

                else:
                    dict2[key] = value 
    except:
        dict2 = key_value
    return dict2

###############################################################################################
# upload invoice images into s3 bucket 
def upload_to_s3(body, bucket, key, content_type):

    upload = s3_client.put_object(Body=body, Bucket=bucket, Key=key,
                ContentType = content_type)
    return upload

###############################################################################################
# process the iploaded file with Textract
def process_text_detection(file_list, client):
    
    agg_field = []
    for file in file_list:
        response = textract_client.analyze_expense(Document={'S3Object': {
            'Bucket': bucket_name,
            'Name': file}})

        fields = {}         # store uploaded file name in key = 'filename'
        fields['filename'] = file

        dict_table = get_table(response)  # dictonary of table
        dict_initial = get_key_value_pairs(response)    # dictonary of key value pairs 
        dict1 = word_to_digit (dict_initial)        # converted word amont to digit
        dict2  = key_correction(dict_table)     # changed all necesary keys 
                       
        fields_new = {**fields, **dict1} # merge dictonaries 
        fields = {**fields_new, **dict2}
 
        # remove keys where value is "" (empty string)
        for key, value in list(fields.items()):
            if (value == ""):
                del fields[key]

        agg_field.append(fields)
    cols = []   # make column names
    for c in agg_field:
        cols = cols + list(c.keys())
    cols = list(dict.fromkeys(cols))
    return agg_field, cols

###############################################################################################
# convert output into csv file and upload csv file to s3
def csv_maker(list_of_dicts,cols):
    keys = cols
    output_filename = str(uuid.uuid4())+ '.csv'
    key = 'output_csv/'+ output_filename 

    with open(output_filename , 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

    s3.meta.client.upload_file(Filename = output_filename, Bucket= bucket_name, Key = key)
    os.remove(output_filename)
    return key    
    
###############################################################################################
# send otp
def send_otp(email, otp):
    otp = str(otp)
    response = cognito_client.admin_create_user(UserPoolId='ap-south-1_WKV3d6Qhh',Username = email,
                    UserAttributes=[{'Name': 'email','Value': email},],TemporaryPassword=otp,
                        DesiredDeliveryMediums=['EMAIL'])
    return otp
    
###############################################################################################
