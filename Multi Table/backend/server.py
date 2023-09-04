import os
from io import BytesIO
from sdv.metadata import MultiTableMetadata
from flask_cors import CORS
from flask import Flask, jsonify, request, send_file,send_from_directory
from werkzeug.utils import secure_filename
import pandas as pd
from sdv.multi_table import HMASynthesizer
from sdv.evaluation.multi_table import evaluate_quality
import json
import zipfile

# app instance
app = Flask(__name__)
CORS(app)


UPLOAD_FOLDER = r'..\Files'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'json','xlsx'}


#report_score
synthetic_data_score = 0.00

#synthetic file name
synthetic_file_name = ''

#global_metdata
metadata ={}

#global_pandas_dataframe
pd_data_frame =[]

#global metadata from SQL
sql_metadata = {}

#merge_sql_type dict
sql_dict ={}

#data
synthetic_data = {}
real_data_frame = {}


#list to store uploaded file paths  
uploaded_files = []       #['filename.json','filename.json',...]

#validation check for primary key
check_set_primary_key = False


# Helper function to check if the file extension is allowed
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



# Enable CORS for all routes
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = 'http://localhost:5173'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, DELETE'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response




#/api/upload-metadata         
@app.route('/api/upload-metadata',methods=['POST']) #need to change from GET to POST
def upload_metadata():
    print('hit -> ./api/upload-metadata')
    if 'file' not in request.files:
        return {'error':'No file Uploaded'},400
    
    print(request)
    file = request.files['file']


    if file.filename == '':
        return {'error':'No selected file'},400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'],filename)
        file.save(file_path)
        
        global sql_metadata
        metadata_file_path = os.path.join(app.config['UPLOAD_FOLDER'],filename)
        with open(metadata_file_path,'r') as file:
            metadata_json_data = file.read()

        #Parse the JSON data using json.loads()
            sql_metadata = json.loads(metadata_json_data)
        print(type(sql_metadata))
        print(sql_metadata)
        os.remove(metadata_file_path)
        return jsonify(sql_metadata)





# /api/upload-data
@app.route('/api/upload-data', methods=['POST'])
def upload_data():
    print('hit-->/api/upload-data ')
    if 'files[]' not in request.files:
        return {'error': 'No file part'}, 400

    files = request.files.getlist('files[]')
    filenames = []      #[filenae.json,failename.json,failname.json,filename.json]

    for file in files:
        if file.filename == '':
            return {'error': 'No selected file'}, 400

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)

            print(filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            filenames.append(filename)

    if filenames:
        global uploaded_files
        uploaded_files.extend(filenames)  # Use extend instead of assignment to add multiple files
        print(uploaded_files)      #save file globally ['filename.xlsx','filename.xlsx','filename.xlsx'] 
        generate_metadata()
        modify_metadata()
        return {'message': f'{len(filenames)} file(s) uploaded successfully', 'filenames': filenames}
    else:
        return {'error': 'Invalid file'}, 400








#called generate metadata after hitting /api/upload-data  
def generate_metadata():
    global metadata, pd_data_frame, real_data_frame

    print('pd_data_frame:',pd_data_frame)
    metadata = {}

    metadata = MultiTableMetadata()
    print(metadata)

    for file in uploaded_files:
        pd_data_frame.append(pd.read_json(os.path.join(app.config['UPLOAD_FOLDER'], file)))
        # pd_data_frame.append(pd.read_excel(os.path.join(app.config['UPLOAD_FOLDER'], file)))
    
    #print(pd_data_frame)
    
    for i, file in enumerate(uploaded_files):
        real_data = pd_data_frame[i]
        temp = file.split('.')
        
        metadata.detect_table_from_dataframe(
            table_name=temp[0],
            data=real_data
        )
        real_data_frame[temp[0]] = real_data

        print('real_data', real_data)
        print('file:', file)
    
    print('metadata',metadata)
    print('pandas data frame',pd_data_frame)
    print('real_data frame',real_data_frame)
    return jsonify(metadata.to_dict())


#updating the metadata according to the json_metadata
def modify_metadata():
    global metadata,sql_metadata,sql_dict
    # print(sql_dict)
    # print(sql_metadata)
    # print(metadata)

    #setting primary key
    for key in sql_metadata.keys():
        if key == 'tables':
            # print('key->',key)
            tables = sql_metadata[key]
            # print('tables->',tables)
            for tb_key in tables.keys():
                # print('table name->',tb_key)
                table_data = tables[tb_key]
                sql_dict[tb_key] = {}   #initialize the dictonary for the table
                # print('table data->',table_data)
                for att_key in table_data.keys():
                    if att_key == 'columns':
                        # print('attribute name->',att_key)
                        column_data = table_data[att_key]
                        # print('column data->data',column_data)
                        for col_name in column_data.keys():
                            # print('column name->',col_name)
                            # print('sql datatype',column_data[col_name])
                            sql_dict[tb_key][col_name]=column_data[col_name]



                    # print('entering into primary key')
                    if att_key == 'primary_key':
                        column_name =table_data[att_key]
                    #    print('column name',column_name)
                        metadata.update_column(
                            table_name = tb_key,
                            column_name=column_name,
                            sdtype='id'
                        )
                        metadata.set_primary_key(
                            table_name=tb_key,
                            column_name=column_name
                        )
                    if att_key == 'foreign_key':
                        column_name=table_data[att_key]
                        metadata.update_column(
                            table_name=tb_key,
                            column_name=column_name,
                            sdtype='id'
                        )


    #setting relationship
    parent_table = ''
    parent_primary_key = ''
    for key in sql_metadata.keys():
        relation_data = sql_metadata[key]
        for rl_key in relation_data.keys():
            if rl_key == 'parent':
                parent_data = relation_data[rl_key]
                for par_key in parent_data.keys():
                    parent_table=par_key
                    parent_primary_key = parent_data[par_key]
    # print(parent_table)
    # print(parent_primary_key)
    
    for key in sql_metadata.keys():
        relation_data = sql_metadata[key]
        for rl_key in relation_data.keys():
            if rl_key == 'child':
                child_data = relation_data[rl_key]
                for child_key in child_data.keys():
                    metadata.add_relationship(
                        parent_table_name=parent_table,
                        child_table_name=child_key,
                        parent_primary_key=parent_primary_key,
                        child_foreign_key=child_data[child_key]
                    )

    # print('sql-dict',sql_dict)
    # print('metadata',metadata)
    # print(metadata.validate())







#/api/get-metadata
@app.route('/api/get-metadata',methods=['GET'])
def get_metadata():
    global metadata,sql_dict
    if metadata == {}:
        return 
    dict_metadata = metadata.to_dict()
    dict_metadata['sql_types']=sql_dict   #joined sql_data_types
    return jsonify(dict_metadata)



#update sdtype 
#/api/update-metadata
@app.route('/api/update-metadata',methods=['POST'])
def update_metadata():
    global metadata
    data = request.json
    print(type(data))
    print(data)

    #uncomment the below code to synthesize any file
    for table_name in data.keys():
        table = data[table_name]
        for col_name in table.keys():
            column = table[col_name]
            for field in column.keys():
                if field == 'sdtype':
                    sdtype = column[field]
                    if sdtype == 'datetime':
                        continue
                        metadata.update_column(
                            table_name=table_name,
                            column_name=col_name,
                            sdtype=sdtype,
                            datetime_format='%Y-%m-%d'
                        )
                    elif sdtype == 'numerical':
                        continue
                        metadata.update_column(
                            table_name=table_name,
                            column_name=col_name,
                            sdtype=sdtype,
                            computer_representation='Float'
                        )
                    else:
                        metadata.update_column(
                            table_name=table_name,
                            column_name=col_name,
                            sdtype=sdtype
                        )

    
    for table_name in data.keys():
        table = data[table_name]
        for col_name in table.keys():
            column = table[col_name]
            for field in column.keys():
                if field == 'pii':
                    if data[table_name][col_name]['sdtype'] in ['email','address','email','phone_number']:
                        metadata.update_column(
                            table_name=table_name,
                            column_name=col_name,
                            pii=column[field]
                        )
    print('successfully update the metadata')    
    print(metadata)

    #uncomment till here


    #hardcode for roadshow
    #comment the below code to synthesize any files
    #updating metadata for EMPLOYEE_TABLE
    # metadata.update_column(
    #     table_name='EMPLOYEE_TABLE',
    #     column_name='EMPLOYEE_ID',
    #     sdtype='id',
    #     regex_format='A[0-9]{6}'
    # )
    # metadata.update_column(
    #     table_name='EMPLOYEE_TABLE',
    #     column_name='FIRST_NAME',
    #     sdtype='first_name'
    # )
    # metadata.update_column(
    #     table_name='EMPLOYEE_TABLE',
    #     column_name='LAST_NAME',
    #     sdtype='last_name'
    # )
    # metadata.update_column(
    #     table_name='EMPLOYEE_TABLE',
    #     column_name='EMAIL_ID',
    #     sdtype='email'
    # )
    # metadata.update_column(
    #     table_name='EMPLOYEE_TABLE',
    #     column_name='JOINING_DATE',
    #     sdtype='datetime',
    #     datetime_format='%Y-%m-%d'
    # )

    # #updating metadata of EXPENSE_TABLE
    # metadata.update_column(
    #     table_name='EXPENSE_TABLE',
    #     column_name='EMPLOYEE_ID',
    #     sdtype='id',
    #     regex_format='A[0-9]{6}'

    # )
    # metadata.update_column(
    #     table_name='EXPENSE_TABLE',
    #     column_name='EXPENSE_ID',
    #     sdtype='id',
    #     regex_format='[0-9]{5}'

    # )
    # metadata.update_column(
    #     table_name='EXPENSE_TABLE',
    #     column_name='EXPENSE_DATE',
    #     sdtype='datetime',
    #     datetime_format='%Y-%m-%d'
    # )

    # #updating metadata for PROJECT_TABLE
    # metadata.update_column(
    #     table_name='PROJECT_TABLE',
    #     column_name='EMPLOYEE_ID',
    #     sdtype='id',
    #     regex_format='A[0-9]{6}'

    # )
    # metadata.update_column(
    #     table_name='PROJECT_TABLE',
    #     column_name='PROJECT_ASSIGNMENT_ID',
    #     sdtype='id'

    # )
    # metadata.update_column(
    #     table_name='PROJECT_TABLE',
    #     column_name='START_DATE',
    #     sdtype='datetime',
    #     datetime_format='%Y-%m-%d'
    # )

    # #updating metadata for SALARY_TABLE
    # metadata.update_column(
    #     table_name='SALARY_TABLE',
    #     column_name='EMPLOYEE_ID',
    #     sdtype='id',
    #     regex_format='A[0-6]{6}'

    # )
    # metadata.update_column(
    #     table_name='SALARY_TABLE',
    #     column_name='ACCOUNT_NO',
    #     sdtype='id',
    #     regex_format='[0-9]{6}'

    # )


    # print(metadata)








                    



    
    return {'message':'update successfully'}

    #comment upto above code





#/api/synthesize
@app.route('/api/synthesize',methods=['GET'])
def sdv_synthesize():
    global real_data_frame,synthetic_data_score,synthetic_data

    print('hit -> /api/synthesize')
    # data = request.json
    # print(type(data))
    # print(data)

    print('real_data_frame',real_data_frame)
    print('synthetic_data',synthetic_data)
   
    print('checking',metadata.validate())
    synthesizer = HMASynthesizer(metadata)
    synthesizer.fit(real_data_frame)



    synthetic_data = synthesizer.sample(scale=2)
    quality_report = evaluate_quality(real_data=real_data_frame,
                                      synthetic_data=synthetic_data,
                                      metadata=metadata)
    print(quality_report)
    synthetic_data_score =round(quality_report.get_score()*100,2)
    print(synthetic_data_score)
    return jsonify({'synthetic_data_score':synthetic_data_score})



#/api/synthetic-score
@app.route('/api/synthetic-score',methods=['GET'])
def get_synthetic_score():
    global synthetic_data_score
    return jsonify({'synthetic_score':synthetic_data_score})



#/api/download/
@app.route('/api/download',methods=['GET'])
def return_synthetic_file():
    global synthetic_data,metadata,pd_data_frame
    file_download = []
    for key in synthetic_data.keys():
        data_frame_table = synthetic_data[key]
        df = pd.DataFrame.from_dict(data_frame_table)
        synthetic_name = key +'-synthetic.xlsx'
        file_path = os.path.join(app.config['UPLOAD_FOLDER'],synthetic_name) 
        df.to_excel(file_path,index=False)   
        print(file_path)
        print(synthetic_name)
        file_download.append(file_path)

    #Create a BytesIO object to store the zip file
    zip_buffer = BytesIO()

    #Create a ZipFile object to write the files to 
    with zipfile.ZipFile(zip_buffer,'w',zipfile.ZIP_DEFLATED) as zip_file:
        for file_path in file_download:
            file_name = os.path.basename(file_path)
            zip_file.write(file_path,arcname=file_name)
            os.remove(file_path)

    zip_buffer.seek(0)

    #remove stored real file 
    for real_file in uploaded_files:
        file_path = os.path.join(app.config['UPLOAD_FOLDER'],real_file)
        print(file_path)
        os.remove(file_path)
    
    # reset_config()

    # Return the zip file as a response
    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True,download_name='synthetic_files.zip')








# Reset everything
@app.route('/api/reset', methods=['GET'])
def reset_config():
    global pd_data_frame, metadata, check_set_primary_key, uploaded_files, synthetic_data_score, synthetic_data
    pd_data_frame = []
    metadata = {}
    check_set_primary_key = False
    uploaded_files = []
    synthetic_data_score = 0.00
    synthetic_data = {}
    return jsonify({'message': 'Metadata and other variables have been reset successfully.'})






#runner
if __name__ == "__main__":
    app.run(debug=True)
