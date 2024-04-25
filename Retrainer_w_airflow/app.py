from flask import Flask, render_template, request, jsonify,redirect, url_for
import json
import joblib
import pandas as pd
from custom_preprocessor import CustomPreprocessor
from Model_Source_Code import train_and_evaluate_model
import os
from git import Repo
import mysql.connector
from sqlalchemy import create_engine
from airflow.models import Variable # type: ignore


def get_current_date(current_period):
    current_period_formatted = '{:02d}'.format(current_period)
    current_date = '2017-08-{}'.format(current_period_formatted)
    return current_date


app = Flask(__name__, static_folder='static')

# Define an empty DataFrame at the beginning
prediction_df = pd.DataFrame(columns=['booking_id', 'predicted_cancellation'])

@app.route('/')
def home():
    # Load analysis results from the JSON file
    with open('analysis_results.json') as infile:
        analysis_results = json.load(infile)
    
    # Pass the results to the template
    return render_template('index.html', analysis_results=analysis_results, prediction_df=prediction_df)

# Load the model
# model = joblib.load("model_pipe.joblib")
@app.route('/predict', methods=['POST'])
def predict():
    global prediction_df
    # Prediction logic
    current_period = int(Variable.get('current_period', default_var=0)) - 1
    current_date = get_current_date(current_period)
    print(current_date)
    db_datawarehouse = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='password',
        database='hotel_datawarehouse'
    )

    str_sql_predictions = f'''
        SELECT bp.booking_id, bp.predicted_cancellation FROM booking_predictions bp
        LEFT JOIN hotel_bookings hb on hb.booking_id = bp.booking_id
        WHERE hb.booking_date = '{pd.Timestamp(current_date)}'
    '''

    prediction_df = pd.read_sql(sql=str_sql_predictions, con=db_datawarehouse)
    db_datawarehouse.close()

    # Save prediction results as CSV
    prediction_csv_path = 'prediction_results.csv'
    prediction_df.to_csv(prediction_csv_path, index=False)

    # Render the prediction results as a table in the template
    return render_template('index.html', prediction_df=prediction_df)

@app.route('/retrain', methods=['POST'])
def retrain():

    #Get the latest source code from Github
    repo_url = 'https://github.com/Anshumaan031/mlops-hotel-cancellation.git'
    local_dir = 'Model_Source_Code'
    
    # Clone or pull the repository to the local directory
    if not os.path.exists(local_dir):
        Repo.clone_from(repo_url, local_dir)
    else:
        repo = Repo(local_dir)
        repo.remotes.origin.pull()

    # Import the updated train_and_evaluate_model function from the Retrainer branch
    from Model_Source_Code import train_and_evaluate_model

    global Traindf
    Traindf = pd.read_csv('hotel_bookings_train_data.csv', delimiter=';')
    Traindf['booking_date'] = pd.to_datetime(Traindf['booking_date'], format='%d/%m/%Y')
    Traindf['arrival_date'] = pd.to_datetime(Traindf['arrival_date'], format='%d/%m/%Y')
    Traindf['booking_date'] = pd.to_datetime(Traindf['booking_date'], format='%d/%m/%Y')
    Traindf['reservation_status_date'] = pd.to_datetime(Traindf['reservation_status_date'], format='%d/%m/%Y')

    trained_model, y_prob, class_report = train_and_evaluate_model(Traindf)

    # Add a message indicating that the model has been retrained
    retrain_message = "Model retrained successfully!"
    retrain_message += "\n" + str(class_report)

    # Render the template with the retrain message
    return redirect(url_for('retrain_success', class_report=class_report))

@app.route('/retrain_success')
def retrain_success():
    # Retrieve the class_report from the URL parameters
    class_report = request.args.get('class_report')

    # Render the retrain success page with the class_report
    return render_template('retrain_success.html', class_report=class_report)


if __name__ == '__main__':
    app.run(debug=True)
