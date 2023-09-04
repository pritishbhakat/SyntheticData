import os
from io import BytesIO
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.evaluation.single_table import evaluate_quality
from flask_cors import CORS
from flask import Flask, jsonify, request, send_file
from werkzeug.utils import secure_filename
import pandas as pd

# app instance
app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = r'C:\Users\A744580\OneDrive - FIL\Documents\Single Table\File'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
ALLOWED_EXTENSIONS = {'xlsx', 'xls','csv'}


#report_score
synthetic_data_score = 0.000000

#synthetic file name
synthetic_file_name = ''

#user config
user_details = {}
primary_key = ''
num_rows =''
pii_array=[]
filename = ''



# Helper function to check if the file extension is allowed
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



# /api/upload
@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return {'error': 'No file part'}, 400

    file = request.files['file']
    print(file.filename)

    if file.filename == '':
        return {'error': 'No selected file'}, 400

    if file and allowed_file(file.filename):
        global filename
        filename = secure_filename(file.filename)
        file_path=os.path.join(app.config['UPLOAD_FOLDER'],filename)
        file.save(file_path)
        #synthetic_data_path = data_vault(filename)
        # os.remove(file_path)
        return {'message': 'File uploaded successfully'}
    else:
        return {'error': 'Invalid file'}, 400
    


# /api/user-config
@app.route('/api/user-config',methods=['POST'])
def return_user_config():
    print(request.json)
    global user_details,primary_key,num_rows,pii_array
    user_details = request.json          #{primary-key:'',nums_rows:0,pii_}
    primary_key = user_details.get('primary_key')
    num_rows = user_details.get('num_rows')
    pii_array = user_details.get('pii_array',[])
    data_vault(filename)
    return {'message':f'Synthetic Quality Score :{synthetic_data_score}.  {num_rows} row is Generated and Ready for download'}









# /api/download
@app.route('/api/download', methods=['GET'])
def download_file():
    synthetic_data_path = os.path.join(app.config['UPLOAD_FOLDER'], synthetic_file_name)
    print(synthetic_data_path)
    # with open(synthetic_data_path,'rb') as f:
    #     file_data=BytesIO(f.read())
    return send_file(synthetic_data_path, as_attachment=True)



# api/quality-report
@app.route("/api/quality-report", methods=['GET'])
def return_quality_report():
    print(jsonify(synthetic_data_score))
    return jsonify(synthetic_data_score)



# sdv
def data_vault(file_name):
    global synthetic_file_name
    global synthetic_data_score
    real_data = pd.read_excel(os.path.join(app.config['UPLOAD_FOLDER'], file_name))
    metadata = SingleTableMetadata()
    metadata.detect_from_dataframe(data=real_data)
    print(metadata)
    metadata.update_column(
        column_name=primary_key,
        sdtype='id'
    )

    for col in pii_array :
        metadata.columns[col]['ppi']=True

    # metadata.columns['guest_email']['pii'] = True
    # metadata.columns['checkin_date']['sdtype'] = 'datetime'
    # metadata.columns['checkout_date']['sdtype'] = 'datetime'
    # metadata.columns['checkin_date']['datetime_format'] = '%d %b %Y'
    # metadata.columns['checkout_date']['datetime_format'] = '%d %b %Y'

    metadata.set_primary_key(column_name=primary_key)

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(real_data)
    synthetic_data = synthesizer.sample(num_rows=num_rows)

    quality_report = evaluate_quality(
        real_data,
        synthetic_data,
        metadata
    )
    print(quality_report)
    synthetic_data_score =round(quality_report.get_score()*100,2)
    split_text = file_name.split('.')
    synthetic_file_name = split_text[0]+'_synthetic.'+split_text[1]
    print(synthetic_file_name)
    synthetic_data_path = os.path.join(app.config['UPLOAD_FOLDER'], synthetic_file_name)
    synthetic_data.to_excel(synthetic_data_path, index=False)
    return synthetic_data_path





#runner
if __name__ == "__main__":
    app.run(debug=True)
