# import all necessary modules 
import csv
import uuid
import utils
import boto3
from word2number import w2n
import numpy as np
from sqlalchemy.orm import sessionmaker
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from flask.templating import render_template
from flask import Flask, request, redirect, url_for,  Response , jsonify
from flask import make_response
from trepan.api import debug
from flask_cors import CORS, cross_origin
import logging
from datetime import datetime
from OpenSSL import SSL

##########################################################################################################
# Initialize Boto3  session and Textract client 

session = boto3.Session()
bucket_name = 'invoice-extraction-files' 
region_name='ap-south-1'

textract_client = session.client(service_name='textract', region_name=region_name)
s3_client = session.client(service_name = 's3', region_name = region_name)
s3 = boto3.resource('s3')
my_bucket = s3.Bucket(bucket_name)

#############################################################################################################
# Initialize app

app = Flask(__name__)
CORS(app, supports_credentials = True)
logging.getLogger('flask_cors').level = logging.DEBUG

app.config['SQLALCHEMY_DATABASE_URI']= 'postgresql://postgres@127.0.0.1/invoice_users'
try:
    db = SQLAlchemy(app)
    engine = create_engine('postgresql://postgres@127.0.0.1/invoice_users')
    Session = sessionmaker(bind = engine)
    print("connected to database.")
except Exception as e:
    print(e)

##########################################################################################################
# Create a database and add data

class user(db.Model):
    __tablename__ = "user_details"

    user_id = db.Column(db.String(32), primary_key = True)
    email = db.Column(db.String(50), nullable =False)
    first_name = db.Column(db.String(50))
    company_name = db.Column(db.String(50))
    otp_verified = db.Column(db.Boolean, nullable =False, default = False)
    download = db.Column(db.Boolean, nullable =False, default = False)
    otp = db.Column(db.Integer)
    output_key = db.Column(db.String(60))
    date_time = db.Column(db.String(50)) 

# add email, company name, first_name and otp in database
    @staticmethod
    def enter_email(data):
        
        session = Session()
        user_id = uuid.uuid4().hex
        email = data["email"]
        first_name = data["first_name"] if "first_name" in data.keys() else None
        company = data["company"] if "company" in data.keys() else None

        date_now = datetime.now()
        otp = np.random.randint(100000,999999)
        row = db.session.query(user).filter(user.email == email).first()
        
        if row is None:
            new_user = user(user_id = user_id, email = email, first_name = first_name, company_name = company, otp=otp, date_time = date_now)
            session.add(new_user)
            session.commit()
        else:
            response = jsonify({"message":"Email already exists"})
            return response
        utils.send_otp(email,otp)
        response = jsonify({"message":"Email added succesfully."})
        return response

###################################################################################################### 
# download output csv file
    @staticmethod
    def download_csv(email):
        session = Session()
        row = db.session.query(user).filter(user.email == email).first()
        row_dict = row.__dict__
        output_key = row_dict['output_key']
        response = s3_client.generate_presigned_url('get_object',Params={'Bucket': bucket_name,
                                                    'Key': output_key},ExpiresIn=180)
        session.commit()
        return response
###################################################################################################### 

@app.route('/')
def index():
    return render_template('SAP_Final.html')

########################################################################################################
# Get user details 

@app.route('/submit', methods = ['POST'])
@cross_origin()
def submit():
    
    if request.method == 'POST':
        requests = request.form.to_dict()
        message = user.enter_email(requests)
        return message

######################################################################################################
# verfy OTP from user

@app.route('/verify_otp', methods = ['POST'])
@cross_origin()
def verify_otp():
    if request.method == 'POST':
        requests = request.form.to_dict()
        email = request.args.get('email')

        session = Session()
        row = db.session.query(user).filter(user.email == email).first()
        if row is None:
            return jsonify({"message":"Invalid entry"})
        row_dict= row.__dict__
        
        user_otp = str(requests['otp'])
        dev_otp = str(row_dict['otp'])
        if dev_otp == user_otp:
            session.query(user).filter(user.email ==email).update({'otp_verified':True})
            session.commit()
            response = jsonify({"message":"OTP is vefied"})
            return response
        else:
            response = jsonify({"message":"Incorrect OTP!!"})
    return response

#######################################################################################################
# upload invoice images

@app.route('/upload_images', methods = ['POST'])
@cross_origin()
def home():
    if request.method == 'POST':       
        email = request.args.get('email')
        upload_files = request.files.getlist('image_name')      
        
        session = Session()
        row = db.session.query(user).filter(user.email == email).first()
        if row is None:
            return jsonify({"message":"Invalid entry"})
        
        row_dict= row.__dict__

        user_id = row_dict['user_id']
        session.commit()

        file_list = []
        for fl in upload_files:
            file_type = fl.filename.split('.')[-1]
            file_name = fl.filename.split('.')[0]
            unique_filename = uuid.uuid4() #generate uuid
            key = 'invoice_images/{}/{}+{}.{}'.format(user_id, unique_filename,file_name, file_type) 
            upload  = utils.upload_to_s3(fl, bucket_name, key, file_type)
            file_list.append(key)
    
        return jsonify({"message":"uploaded sucessfully."})

####################################################################################################
#analyze uploaded images and write csv file

@app.route('/analyze', methods = ['GET'])
def analyze():
    
    email = request.args.get('email')
    session = Session()
    row = db.session.query(user).filter(user.email == email).first()
    if row is None:
        return jsonify({"message":"Invalid entry"})
    row_dict= row.__dict__
    user_id = row_dict['user_id']

    if request.method == 'GET':
        folder = 'invoice_images/{}'.format(user_id)
        file_list = []
        for object_summary in my_bucket.objects.filter(Prefix=folder):
            file_list.append(object_summary.key)

        extracted_fields = utils.process_text_detection(file_list, textract_client)
        print(extracted_fields)
        print('********\n')
        output_key = utils.csv_maker(extracted_fields)
        print(output_key)
        print('****\n')
        session.query(user).filter(user.email ==email).update({'output_key':output_key})
        session.commit()
        response = jsonify({"message":"Output is ready to download."})
        response.headers.add("Access-Control-Allow-origin","*")
    return response

####################################################################################################
#download csv_output from s3

@app.route('/download', methods = ['GET'])
def download():
    email = request.args.get('email')
    session = Session()    
    row = db.session.query(user).filter(user.email == email).first()
    if row is None:
        return jsonify({"message":"Invalid entry"})
    row_dict = row.__dict__
    download = row_dict['download']
    
    if download == False:
        response = user.download_csv(email)
        session.query(user).filter(user.email ==email).update({'download':True})
        session.commit()
        return response
    else:
        response1 = jsonify({"message":"already downloaded"})
        response1.headers.add("Access-Control-Allow-Origin", "*")
        return response1

#########################################################################################

if __name__  == '__main__':
    app.run(debug=True)
    