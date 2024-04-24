Here is all documents to run the MLOps until Apr 12. 

You will need hotel_bookings.csv. 

**ML OPS**

In the MLOps Notebook, I split the data into "old" (hotel_bookings) and "new" (data_profiling_before) data. The data_profiling_before takes only the bookings of one day. 

This code also downloads as csv the new data, without the is_cancelled column, to later perform predictions. The document is called data_to_predict.csv.

MLOps Notebook does some data analysis + input features drift detection. It downloads the messages as a JSON at the end called analysis_results.json

**FLASK**
The Flask App takes input from the analysis_results.json and prints the messages. 
At the end, there is a button for the user to click to generate and print predictions.

It works by defining values, and then passing them to an HTML file (index.HTML). There is also a CSS and a JavaScript files.  

HOW? 

**Predictions**
I trained a model in the Jupyter Notebook file Meri_4301_Model in the directory. It has two steps, a CustomPreprocessor, and an AdaBoost Classifier. Once trained, I downlaoded it as model_pipe.joblib. The idea is to be able to pass the data_to_predict.csv directly and make predictions (Since part of the pipe is the preprocessing steps already). 

However, to use the model from Flask, I needed to create a file called 'custom_preprocessor.py' containing the preprocessor steps. 

When the button "Make Predictions" is clicked in index.html, a JavaScript script runs to trigger the predict() function defined in Flask.

The app is stateless, so I need to refresh the site to see the output in the HTML. 


**to run the app**

python app.py