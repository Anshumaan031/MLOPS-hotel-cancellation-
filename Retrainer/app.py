from flask import Flask, render_template, request, jsonify,redirect, url_for
import json
import joblib
import pandas as pd
from custom_preprocessor import CustomPreprocessor
from Model_Source_Code import train_and_evaluate_model
import os
from git import Repo 

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
model = joblib.load("model_pipe.joblib")
@app.route('/predict', methods=['POST'])
def predict():
    global prediction_df
    # Prediction logic
    data_df = pd.read_csv('data_to_predict.csv')

    # Apply preprocessing steps
    #preprocessor = CustomPreprocessor()
    #data_df_preprocessed = preprocessor.transform(data_df)

    # Apply the same filter as in the model pipeline
    filter = (data_df['children'] == 0) & (data_df['adults'] == 0) & (data_df['babies'] == 0)
    data_df_filtered = data_df[~filter]

    # Extract booking IDs
    booking_ids = data_df_filtered['booking_id']

    # Predict cancellation
    prediction_results = model.predict(data_df_filtered)

    # Create a DataFrame with booking IDs and predicted cancellation
    prediction_df = pd.DataFrame({'booking_id': booking_ids, 'predicted_cancellation': prediction_results})

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
