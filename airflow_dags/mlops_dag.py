from datetime import datetime, timedelta
from airflow.models.dag import DAG # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.python_operator import BranchPythonOperator # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor # type: ignore
from airflow.operators.python import ShortCircuitOperator # type: ignore
from airflow.operators.dummy_operator import DummyOperator # type: ignore
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowException, AirflowFailException # type: ignore
import pyarrow
import json
import scipy.stats as stats
from flask import Flask, render_template, request, jsonify
import json
import requests
import joblib
import pandas as pd
import subprocess
import time


def get_current_date(current_period):
    current_period_formatted = '{:02d}'.format(current_period)
    current_date = '2017-08-{}'.format(current_period_formatted)
    return current_date


def read_data_from_warehouse():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df_all = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    current_period = int(Variable.get('current_period', default_var=0)) - 1
    current_date = get_current_date(current_period)
    historical_date = '2017-07-31'
    df = df_all[df_all['booking_date'] == pd.Timestamp(current_date)]
    df_hist = df_all[df_all['booking_date'] <= pd.Timestamp(historical_date)]
    db_datawarehouse.close()
    return df, df_hist


def input_drift_detection():
    df, df_hist = read_data_from_warehouse()
    print("checking shape!!!!!!!!!!!!")
    print(df.shape)
    cat_col = ['meal','country','market_segment','distribution_channel','is_repeated_guest','reserved_room_type','assigned_room_type','deposit_type','agent','customer_type']
    cat_col_eval = ['meal','market_segment','distribution_channel','is_repeated_guest','reserved_room_type','assigned_room_type','deposit_type','customer_type']
    numerical_cols = ['lead_time','stays_in_weekend_nights','stays_in_week_nights', 'adults', 'babies','previous_cancellations','previous_bookings_not_canceled','days_in_waiting_list','adr','total_of_special_requests']
    binary_cols = ['is_canceled','is_repeated_guest','required_car_parking_spaces']
    categorical_cols = ['hotel','meal','country', 'market_segment', 'distribution_channel','reserved_room_type','assigned_room_type', 'booking_changes', 'deposit_type', 'agent','customer_type','reservation_status']
    time_cols = ['booking_id','arrival_date_year', 'arrival_date_month', 'arrival_date_week_number','arrival_date_day_of_month','reservation_status_date', 'arrival_date', 'booking_date']

    # performing correlation analysis
    print('Correlation Analysis')
    message_correlation_analysis = 'Correlation Analysis'
    print('hotel_bookings:')
    title_hotel_bookings_corr = 'hotel_bookings correlation:'
    correlation_matrix = df[numerical_cols].corr()
    message_hotel_bookings_corr = []

    for i in range(len(correlation_matrix.columns)):
        for j in range(i+1, len(correlation_matrix.columns)):
            if correlation_matrix.iloc[i, j] < -0.4 or correlation_matrix.iloc[i, j] > 0.4:
                alert_message = f"The correlation between {correlation_matrix.columns[i]} and {correlation_matrix.columns[j]} is {correlation_matrix.iloc[i, j]:.2f} //"
                print(alert_message)
                message_hotel_bookings_corr.append(alert_message)

    message_hotel_bookings_corr = "\n".join(message_hotel_bookings_corr)

    # performing data profiling before correlation
    print("data_profiling_before correlation:")
    title_data_profiling_before = "data_profiling_before:"
    correlation_matrix = df[numerical_cols].corr()
    message_data_profiling_before = []
    for i in range(len(correlation_matrix.columns)):
        for j in range(i+1, len(correlation_matrix.columns)):
            if correlation_matrix.iloc[i, j] < -0.4 or correlation_matrix.iloc[i, j] > 0.4:
                alert_message = f"The correlation between {correlation_matrix.columns[i]} and {correlation_matrix.columns[j]} is {correlation_matrix.iloc[i, j]:.2f} //"
                print(alert_message)
                message_data_profiling_before.append(alert_message)

    message_data_profiling_before = "\n".join(message_data_profiling_before)

    # counting number of nulls in each column
    print("Null counts in each column:")
    title_count_nulls = "Null counts in each column:"
    null_counts = df.isnull().sum()
    print(null_counts)

    message_count_nulls = pd.DataFrame({'Column Name': null_counts.index, 'Null Count': null_counts.values})
    message_count_nulls = message_count_nulls.to_html(classes='my-table', index=False, border=0)

    title_count_cat = 'Number of categorical columns'
    print(f"\nCount of categorical columns: {len(categorical_cols)}")
    message_categorical = f"\nCount of categorical columns: {len(categorical_cols)}"
    title_count_num = 'Number of numerical columns'
    print(f"Count of numerical columns: {len(numerical_cols)}")
    message_numerical = f"Count of numerical columns: {len(numerical_cols)}"
    title_count_bin = 'Number of binary columns'
    print(f"Count of binary columns: {len(binary_cols)}") 
    message_binary = f"Count of binary columns: {len(binary_cols)}" 

    # finding outliers
    def detect_outliers_iqr(data):
        mean = data.mean()
        std = data.std()
        lower_bound = mean - 4 * std
        upper_bound = mean + 4 * std

        return data[(data < lower_bound) | (data > upper_bound)]

    outliers_df = pd.DataFrame()
    for col in numerical_cols:
        outliers = detect_outliers_iqr(df[col])
        outliers_df[col] = df[col].isin(outliers)

    outliers_mask = outliers_df.any(axis=1)
    print('Check for outliers --> 4 x standard deviation ')
    title_outliers = 'Check for outliers --> 4 x standard deviation '

    message_outliers_numeric = []
    message_outliers_binary = []

    for index, row in outliers_df.iterrows():
        if row.any(): 
            booking_id = df.loc[index, 'booking_id']
            outlier_columns = row[row].index.tolist()
            outlier_values = df.loc[index, outlier_columns].to_dict()
            outlier_message = f"Booking ID {booking_id} outlier values: {outlier_values} "
            print(outlier_message)
            message_outliers_numeric.append(outlier_message)

        
    print('Check for non-binary values in binary columns')
    for col in binary_cols:
        non_binary_rows = df[~df[col].isin([0, 1])]
        if not non_binary_rows.empty:
            for index, row in non_binary_rows.iterrows():
                booking_id = row['booking_id']
                non_binary_message = f"Non-binary alert for booking ID {booking_id} in column {col}: Value = {row[col]}"
                print(non_binary_message)
                message_outliers_binary.append(non_binary_message)

    merged_outliers_message = message_outliers_numeric + message_outliers_binary
    merged_outliers_message = pd.DataFrame(merged_outliers_message, columns=['Message'])
    merged_outliers_message = merged_outliers_message.to_html(classes='my-table', index=False, border=0)

    # input features drift detection
    print('New Data Exploration for Numerical Columns')
    title_data_drift_num = 'Exploring input fature data drift for numerical columns'
    print('We only study features generated at booking time. For instance, we exclude number of days in waitlist.')
    message_data_drift_num = 'We only study features generated at booking time. For instance, we exclude number of days in waitlist.'
    print('T-TEST AND K-S TEST')
    title_ttest = 'T-TEST RESULTS. Significantly different columns'
    title_kstest = 'K-S TEST RESULTS. Significantly different columns'
    num_cols = [
        'lead_time', 'stays_in_weekend_nights', 'stays_in_week_nights',
        'adults', 'babies', 'previous_cancellations',
        'previous_bookings_not_canceled', 'adr', 'total_of_special_requests'
    ]

    p_values_ttest = {}
    p_values_ks = {}
    means = {}
    p_value_threshold = 0.01

    for col in num_cols:
        # Perform the T-test
        t_stat, p_val_ttest = stats.ttest_ind(
            df_hist[col].dropna(),
            df[col].dropna(),
            equal_var=False
        )
        p_values_ttest[col] = p_val_ttest
        
        # Perform the Kolmogorov-Smirnov test
        ks_stat, p_val_ks = stats.ks_2samp(
            df_hist[col].dropna(),
            df[col].dropna()
        )
        p_values_ks[col] = p_val_ks
        
        mean_hotel_bookings = df_hist[col].mean()
        mean_data_profiling_before = df[col].mean()
        means[col] = (mean_hotel_bookings, mean_data_profiling_before)

    significant_data_ttest = [
        (
            col, 
            p_values_ttest[col], 
            means[col][0],
            means[col][1],
            ((means[col][1] - means[col][0]) / means[col][0] * 100) if means[col][0] != 0 else 'Infinity'
        ) 
        for col in num_cols 
        if p_values_ttest[col] < p_value_threshold
    ]

    significant_cols_df_ttest = pd.DataFrame(
        significant_data_ttest, 
        columns=['Feature', 'P-value (T-test)', 'Old Mean', 'New Mean', 'Percentage Change']
    )

    significant_data_ks = [
        (col, p_values_ks[col]) for col in num_cols if p_values_ks[col] < p_value_threshold
    ]

    significant_cols_df_ks = pd.DataFrame(
        significant_data_ks, 
        columns=['Feature', 'P-value (K-S test)']
    )

    if not significant_cols_df_ttest.empty:
        print('T-TEST Alert! We found significant differences in the new data based on T-test!')
        print(significant_cols_df_ttest)
        message_ttest = significant_cols_df_ttest.to_html(classes='my-table', index=False, border=0)

        print(message_ttest)
    else:
        print(f"No significant differences found at the specified {p_value_threshold} threshold.")
        message_ttest = f"No significant differences found at the specified {p_value_threshold} threshold."

    if not significant_cols_df_ks.empty:
        print('K-S TEST Alert! We found significant differences in the new data based on K-S test!')
        print(significant_cols_df_ks)
        message_kstest = significant_cols_df_ks.to_html(classes='my-table', index=False, border=0)
    else:
        print(f"No significant differences found at the specified {p_value_threshold} threshold.")
        message_kstest = f"No significant differences found at the specified {p_value_threshold} threshold."


    print('New Data Exploration for Categorical Columns')

    title_chi2 = 'CHI SQUARED RESULTS for categorical columns. Significantly different columns'
    print('CHI^2 TEST')

    def chi2_test(cat_col, df1, df2):
        freq_df1 = df1[cat_col].value_counts().to_frame(name='count1')
        freq_df2 = df2[cat_col].value_counts().to_frame(name='count2')
        freq_df = freq_df1.merge(freq_df2, left_index=True, right_index=True, how='outer').fillna(0)

        chi2_stat, p_val, dof, expected = stats.chi2_contingency(freq_df)
        
        return p_val

    cat_col_eval = [
        'meal', 'market_segment', 'distribution_channel', 'is_repeated_guest',
        'reserved_room_type', 'assigned_room_type', 'deposit_type', 'customer_type'
    ]

    print('New Data Exploration')
    p_values_cat = {}

    for col in cat_col_eval:
        p_val = chi2_test(col, df_hist, df)
        p_values_cat[col] = p_val

    significant_cats = {col: p for col, p in p_values_cat.items() if p < p_value_threshold}
    significant_data_cat = [(col, significant_cats[col]) for col in significant_cats]
    significant_cats_df = pd.DataFrame(significant_data_cat, columns=['Feature', 'P-value'])

    if not significant_cats_df.empty:
        print('Alert! We found significant differences in the categorical features of the new data!')
        print(significant_cats_df)
        message_chi2 = significant_cats_df.to_html(classes='my-table', index=False, border=0)
        
        for feature in significant_cats_df['Feature']:
            print(f"\nDistribution for {feature} BEFORE:")
            print(df_hist[feature].value_counts(normalize=True).sort_index())
            
            print(f"\nDistribution for {feature} AFTER:")
            print(df[feature].value_counts(normalize=True).sort_index())
    else:
        print(f"No significant differences in categorical features found at the specified {p_value_threshold} threshold.")
        message_chi2 = f"No significant differences in categorical features found at the specified {p_value_threshold} threshold."

    empty = " "
    analysis_results = [
        {'title': message_correlation_analysis, 'message': empty},
        {'title': title_hotel_bookings_corr, 'message': message_hotel_bookings_corr},
        {'title':title_data_profiling_before, 'message': message_data_profiling_before},
        {'title':title_count_nulls, 'message': message_count_nulls},
        {'title':title_count_cat, 'message': message_categorical},
        {'title':title_count_num, 'message': message_numerical},
        {'title':title_count_bin, 'message': message_binary},
        {'title': title_outliers, 'message': merged_outliers_message},
        {'title': title_ttest, 'message': message_ttest},
        {'title': title_kstest, 'message': message_kstest},
        {'title':title_chi2, 'message':message_chi2}
    ]

    with open('analysis_results.json', 'w') as outfile:
        json.dump(analysis_results, outfile)

    return


def preprocess_test_data(input_df):
    test_data = input_df.copy()
    
    useless_cols = ['days_in_waiting_list', 'arrival_date_year', 'assigned_room_type', 'booking_changes', 'reservation_status', 'country', 'days_in_waiting_list']
    test_data.drop(columns=useless_cols, inplace=True)
    
    test_data.fillna(0, inplace=True)
    
    test_data["arrival_date"] = pd.to_datetime(test_data["arrival_date"])
    test_data["booking_date"] = pd.to_datetime(test_data["booking_date"])
    
    if 'reservation_status_date' in test_data.columns:
        test_data['reservation_status_date'] = pd.to_datetime(test_data['reservation_status_date'])
        test_data['year'] = test_data['reservation_status_date'].dt.year
        test_data['month'] = test_data['reservation_status_date'].dt.month
        test_data['day'] = test_data['reservation_status_date'].dt.day
        test_data.drop(['reservation_status_date', 'arrival_date_month'], axis=1, inplace=True)
    
    mappings = {
        'hotel': {'Resort Hotel': 0, 'City Hotel': 1},
        'meal': {'BB': 0, 'FB': 1, 'HB': 2, 'SC': 3, 'Undefined': 4},
        'market_segment': {'Direct': 0, 'Corporate': 1, 'Online TA': 2, 'Offline TA/TO': 3, 'Complementary': 4, 'Groups': 5, 'Undefined': 6, 'Aviation': 7},
        'distribution_channel': {'Direct': 0, 'Corporate': 1, 'TA/TO': 2, 'Undefined': 3, 'GDS': 4},
        'reserved_room_type': {'C': 0, 'A': 1, 'D': 2, 'E': 3, 'G': 4, 'F': 5, 'H': 6, 'L': 7, 'B': 8},
        'deposit_type': {'No Deposit': 0, 'Refundable': 1, 'Non Refund': 3},
        'customer_type': {'Transient': 0, 'Contract': 1, 'Transient-Party': 2, 'Group': 3},
        'year': {2015: 0, 2014: 1, 2016: 2, 2017: 3}
    }
    
    for col, mapping in mappings.items():
        if col in test_data.columns:
            test_data[col] = test_data[col].map(mapping)
            test_data[col] = test_data[col].fillna(-1)
    
    num_cols = ['lead_time', 'arrival_date_week_number', 'arrival_date_day_of_month', 'agent', 'adr']
    for col in num_cols:
        if col in test_data.columns:
            test_data[col] = np.log(test_data[col] + 1)
    
    columns_to_drop = ['arrival_date', 'booking_date']
    test_data.drop(columns=columns_to_drop, inplace=True)
    
    return test_data


def make_prediction(row):
    url = 'https://ml-hotel-cancellation-prod-d6b6251be054.herokuapp.com/predict'

    payload = {
        "data": {
            "hotel": row['hotel'],
            "meal": row['meal'],
            "market_segment": row['market_segment'],
            "distribution_channel": row['distribution_channel'],
            "reserved_room_type": row['reserved_room_type'],
            "deposit_type": row['deposit_type'],
            "customer_type": row['customer_type'],
            "year": row['year'],
            "month": row['month'],
            "day": row['day'],
            "lead_time": row['lead_time'],
            "arrival_date_week_number": row['arrival_date_week_number'],
            "arrival_date_day_of_month": row['arrival_date_day_of_month'],
            "stays_in_weekend_nights": row['stays_in_weekend_nights'],
            "stays_in_week_nights": row['stays_in_week_nights'],
            "adults": row['adults'],
            "children": row['children'],
            "babies": row['babies'],
            "is_repeated_guest": row['is_repeated_guest'],
            "previous_cancellations": row['previous_cancellations'],
            "previous_bookings_not_canceled": row['previous_bookings_not_canceled'],
            "agent": row['agent'],
            "adr": row['adr'],
            "required_car_parking_spaces": row['required_car_parking_spaces'],
            "total_of_special_requests": row['total_of_special_requests']
        }
    }

    payload_json = json.dumps(payload)
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=payload_json)

    if response.status_code == 200:
        print("API call successful for booking {}!".format(row['booking_id']))
    else:
        print(f"Error: {response.text}")

    return response.json()

# only for testing purposes
def dummy_make_prediction(row):
    response = {'prediction': 'Not Canceled', 'probability': 0.5081928794207051}
    print("API call successful for booking {}!".format(row['booking_id']))
    return json.loads(json.dumps(response))


def get_existing_predictions():
    booking_ids = []
    db_bookingpred = mysql.connector.connect(
	host='localhost',
	user='root',
	passwd='password',
	database='hotel_datawarehouse'
    )

    cursor = db_bookingpred.cursor()
    cursor.execute('SELECT DISTINCT bp.booking_id from booking_predictions bp;')
    rows = cursor.fetchall()
    for row in rows:
        booking_ids.append(row[0])
    
    cursor.close()
    db_bookingpred.close()

    return booking_ids


def show_prediction():
    start_time = time.time()
    df, df_hist = read_data_from_warehouse()
    current_bookingpred_ids = get_existing_predictions()
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()
    df = df.head(5) # we do this because we are using the free version of heroku, and can only a certain amount of API calls per day. In a real environment, this is not needed. So we take 5 bookings per simulated day for demonstration purposes
    # df_temp = df.copy()
    df = preprocess_test_data(df)
    prediction_df = pd.DataFrame(columns=['booking_id', 'predicted_cancellation'])
    # booking_ids = df_temp['booking_id']
    booking_ids = df['booking_id']
    print(booking_ids)
    predictions = []
    probabilites = []
    count = 0
    for index, row in df.iterrows():
        pred = make_prediction(row)
        # pred = dummy_make_prediction(row)
        predictions.append(pred['prediction'])
        probabilites.append(pred['probability'])
        # this is just an extra condition to enforce the 5 API calls per simulated day as talked earlier (since we are using free version of Heroku)
        if count > 4:
            break
        count = count + 1

    prediction_df = pd.DataFrame({'booking_id': booking_ids, 'predicted_cancellation': predictions, 'probability': probabilites})
    print(prediction_df.shape)
    print(prediction_df.iloc[0, :])

    print(current_bookingpred_ids)
    delta_prediction_df = prediction_df[~prediction_df['booking_id'].isin(current_bookingpred_ids)]
    delta_prediction_df = delta_prediction_df.drop_duplicates(subset=['booking_id'])
    print(delta_prediction_df.shape)
    print(delta_prediction_df)
    delta_prediction_df.to_sql(name='booking_predictions', con=db_datawarehouse, if_exists='append', index=False)
    db_datawarehouse.close()

    end_time = time.time()
    execution_time = end_time - start_time
    num_entries = len(df)
    avg_time = execution_time / num_entries

    print("Total time taken for showing predictions:", execution_time)
    print("Number of bookings to predict:", num_entries)
    print("Average time taken to make predictions:", avg_time)

    return


def check_initial_period():
    current_period = int(Variable.get('current_period', default_var=0)) - 1
    if current_period > 0:
        return 'wait_for_dataops_task'
    else:
        return 'end'


with DAG(
    'gp_mlops_2',
    default_args={
        'depends_on_past': False,
        'email': ['685@doonschool.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Group Project ML Ops',

    # ┌───────────── minute (0–59)
    # │ ┌───────────── hour (0–23)
    # │ │ ┌───────────── day of the month (1–31)
    # │ │ │ ┌───────────── month (1–12)
    # │ │ │ │ ┌───────────── day of the week (0–6) (Sunday to Saturday;
    # │ │ │ │ │                                   7 is also Sunday on some systems)
    # │ │ │ │ │
    # │ │ │ │ │
    # * * * * * <command to execute>
    schedule_interval='*/10 * * * *',

    start_date=datetime(2024, 4, 10),
    # dagrun_timeout=timedelta(seconds=60),
    catchup=False,
    tags=["group_project"],
) as dag:
    
    start = DummyOperator(task_id='start')
    # middle = DummyOperator(task_id='middle')
    end = DummyOperator(task_id='end')

    input_drift_detection = PythonOperator(
        task_id='input_drift_detection',
        python_callable=input_drift_detection
    )

    show_prediction = PythonOperator(
        task_id='show_prediction',
        python_callable=show_prediction
    )

    wait_for_dataops_task = ExternalTaskSensor(
        task_id='wait_for_dataops_task',
        external_dag_id='gp_dataops_2',
        external_task_id='data_profiling_after',
        timeout=510,
        mode='reschedule',
        poke_interval=30,
        dag=dag,
    )

    branch_task_check_initial_period = BranchPythonOperator(
        task_id='check_initial_period',
        provide_context=True,
        python_callable=check_initial_period
    )

    start >> branch_task_check_initial_period >> [wait_for_dataops_task, end]
    wait_for_dataops_task >> input_drift_detection >> show_prediction >> end