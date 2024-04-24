from datetime import datetime, timedelta
from airflow.models.dag import DAG # type: ignore
from airflow.models import Variable # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.python_operator import BranchPythonOperator # type: ignore
from airflow.operators.python import ShortCircuitOperator # type: ignore
from airflow.operators.dummy_operator import DummyOperator # type: ignore
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from airflow.exceptions import AirflowException, AirflowFailException # type: ignore
import pyarrow


expected_num_columns = 33
kpi_stop_threshold = 95
kpi_warn_threshold = 96.5


def clean_datawarehouse():
    #  housekeeping - drop existing tables in datawarehouse

    db_datawarehouse = mysql.connector.connect(
	host='localhost',
	user='root',
	passwd='password',
	database='hotel_datawarehouse'
    )

    cursor = db_datawarehouse.cursor()
    cursor.execute('DROP TABLE IF EXISTS hotel_bookings;')
    cursor.execute('DROP TABLE IF EXISTS data_profiling_before;')
    cursor.execute('DROP TABLE IF EXISTS data_profiling_after;')
    cursor.execute('DROP TABLE IF EXISTS booking_predictions;')
    print("=======================")
    print("Cleared all the existing tables in the database hotel_datawarehouse")
    print("\n")

    db_datawarehouse.commit()
    db_datawarehouse.close()

    return


def create_datawarehouse_and_upload_hist():
    db_hotelbookings = mysql.connector.connect(
	host='localhost',
	user='root',
	passwd='password',
	database='hotelbookings'
    )

    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    # ingest data from hotelbookings database

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'

    df = pd.read_sql(sql=str_sql_bookings, con=db_hotelbookings)

    print("=======================")
    df['arrival_date'] = pd.to_datetime(df['arrival_date_year'].astype(str) + '-' +
                                        df['arrival_date_month'] + '-' +
                                        df['arrival_date_day_of_month'].astype(str), format='%Y-%B-%d')
    df['booking_date'] = (df['arrival_date'] - pd.to_timedelta(df['lead_time'], unit='d')).dt.date
    last_month = df['booking_date'].max().month
    last_month = '{:02d}'.format(last_month)
    last_date = '2017-{}-01'.format(last_month)
    df_hist = df[df['booking_date'] < pd.Timestamp(last_date)].copy()
    df_hist.drop(['arrival_date', 'booking_date'], axis=1, inplace=True)
    print(df_hist.shape)
    print(df_hist.columns)

    # load data into hotel_datawarehouse database
    df_hist.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace')

    # load data into data_profiling_before database
    data_report_hist = data_quality_report(df_hist)
    data_report_hist.to_sql(name='data_profiling_before', con=db_datawarehouse, if_exists='replace')

    # create an empty booking_predictions database
    df_bookingpred = pd.DataFrame(columns=['booking_id', 'predicted_cancellation', 'probability'])
    df_bookingpred.to_sql(name='booking_predictions', con=db_datawarehouse, if_exists='replace', index=False)

    db_hotelbookings.close()
    db_datawarehouse.close()
    print("Created and populated the historical data from table hotel_bookings in the database hotel_datawarehouse, shape: {}".format(df_hist.shape))
    print("\n")
    return


def get_current_date(current_period):
    current_period_formatted = '{:02d}'.format(current_period)
    current_date = '2017-08-{}'.format(current_period_formatted)
    return current_date


def initial_load_current_data():
    current_period = int(Variable.get('current_period', default_var=0)) - 1
    current_date = get_current_date(current_period)

    db_hotelbookings = mysql.connector.connect(
	host='localhost',
	user='root',
	passwd='password',
	database='hotelbookings'
    )

    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_hotelbookings)

    print("=======================")
    print(current_date)
    df['arrival_date'] = pd.to_datetime(df['arrival_date_year'].astype(str) + '-' +
                                        df['arrival_date_month'] + '-' +
                                        df['arrival_date_day_of_month'].astype(str), format='%Y-%B-%d')
    df['booking_date'] = (df['arrival_date'] - pd.to_timedelta(df['lead_time'], unit='d')).dt.date
    df_current = df[df['booking_date'] <= pd.Timestamp(current_date)]
    df_current.drop(['arrival_date', 'booking_date'], axis=1, inplace=True)

    df_current.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace')
    db_hotelbookings.close()
    db_datawarehouse.close()
    print("Initial load of data done into the hotel_datawarehouse database:", df_current.shape)
    print(df_current.columns)
    print("\n")

    return


def data_quality_report(df):
    
    if isinstance(df, pd.core.frame.DataFrame):
        
        descriptive_statistics = df.describe(include = 'all')
        data_types = pd.DataFrame(df.dtypes, columns=['Data Type']).transpose()
        missing_value_counts = pd.DataFrame(df.isnull().sum(), columns=['Missing Values']).transpose()
        present_value_counts = pd.DataFrame(df.count(), columns=['Present Values']).transpose()
        # cardinality
        cardinality = pd.DataFrame(df.nunique(), columns=['Cardinality']).transpose()
        # duplicates
        duplicate_counts = {}
        for col in df.columns:
            duplicates = df[col].duplicated().sum()
            duplicate_counts[col] = duplicates
        
        df_dup = pd.DataFrame(duplicate_counts, index=['Duplicate Records'])
        # outliers
        num_cols = df.select_dtypes(include=[np.number]).columns
        df_outliers = pd.DataFrame(columns=num_cols)
        
        for col in df_outliers:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            lb = q1 - 1.5 * iqr
            ub = q3 + 1.5 * iqr
            outliers = df[(df[col] < lb) | (df[col] > ub)][col].count()
            df_outliers.loc['Outliers', col] = outliers

        data_report = pd.concat([descriptive_statistics, data_types, missing_value_counts, present_value_counts, cardinality, df_dup, df_outliers], axis=0)
        data_report = data_report.T
        data_report.reset_index(inplace=True)
        data_report.rename(columns={'index': 'column'}, inplace=True)
        
        return data_report
    
    else:
    
        return None


def data_profiling_before():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)
    df['arrival_date'] = pd.to_datetime(df['arrival_date_year'].astype(str) + '-' +
                                        df['arrival_date_month'] + '-' +
                                        df['arrival_date_day_of_month'].astype(str), format='%Y-%B-%d')
    df['booking_date'] = (df['arrival_date'] - pd.to_timedelta(df['lead_time'], unit='d')).dt.date
    current_period = int(Variable.get('current_period', default_var=0)) - 1
    current_date = get_current_date(current_period)
    df_current = df[df['booking_date'] == pd.Timestamp(current_date)]
    df_current.drop(['arrival_date', 'booking_date'], axis=1, inplace=True)
    print(df_current.shape)

    data_report = data_quality_report(df_current)
    data_report = data_report.iloc[1:, :]
    data_report.to_sql(name='data_profiling_before', con=db_datawarehouse, if_exists='replace')
    print("=======================")
    print("Initial data profiling done - check data_profiling_before table in the database hotel_datawarehouse")
    print("\n")
    db_datawarehouse.close()

    return


def kpi_threshold_check():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['arrival_date'] = pd.to_datetime(df['arrival_date_year'].astype(str) + '-' +
                                        df['arrival_date_month'] + '-' +
                                        df['arrival_date_day_of_month'].astype(str), format='%Y-%B-%d')
    df['booking_date'] = (df['arrival_date'] - pd.to_timedelta(df['lead_time'], unit='d')).dt.date
    current_period = int(Variable.get('current_period', default_var=0)) - 1
    current_date = get_current_date(current_period)
    df_current = df[df['booking_date'] == pd.Timestamp(current_date)]
    df_current.drop(['arrival_date', 'booking_date'], axis=1, inplace=True)
    print(df_current.shape)

    initial_row_count = len(df_current)
    df_current = df_current[df_current['adults'].between(0, 100)]
    df_current = df_current[df_current['stays_in_weekend_nights'].between(0, 20)]
    df_current = df_current[df_current['stays_in_week_nights'].between(0, 50)]
    df_current = df_current[df_current['required_car_parking_spaces'].between(0, 8)] 

    final_row_count = len(df_current)
    compliance_rate = (final_row_count / initial_row_count) * 100
    print(final_row_count)
    print(initial_row_count)
    
    print("=======================")
    if compliance_rate < kpi_stop_threshold:
        raise AirflowFailException(f"Alert: Data compliance rate is below the kpi threshold of {kpi_stop_threshold}%: {compliance_rate}%")
    elif compliance_rate >= kpi_stop_threshold and compliance_rate <= kpi_warn_threshold:
        print(f"Warning: Data compliance rate is meets the kpi threshold of {kpi_stop_threshold}% but is near it: {compliance_rate}%")
    else:
        print(f"Data compliance rate is acceptable and is well above the kpi threshold of {kpi_stop_threshold}%: {compliance_rate}%")
    db_datawarehouse.close()
    print("\n")

    return


def replace_negative_values():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)
    df = df.iloc[:, 1:]

    numerical_columns = df.select_dtypes(include=[np.number]).columns
    df[numerical_columns] = df[numerical_columns].clip(lower=0)

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace')
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced negative values for all numerical columns")
    print("\n")
    db_datawarehouse.close()

    return


def handling_hotel_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['hotel'] = df['hotel'].apply(lambda x: x if x in ["Resort Hotel", "City Hotel"] else "City Hotel")
    df = df.iloc[:, 1:]

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for hotel column by the value - 'City Hotel'")
    print("\n")
    db_datawarehouse.close()

    return


def handling_children_and_babies_cols_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['children'] = df['children'].fillna(lambda x: 0)
    df['babies'] = df['babies'].fillna(lambda x: 0)

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print(df.shape)
    print(df.columns)
    print("=======================")
    print("replaced all the null values for children and babies columns by the value - 0")
    print("\n")
    db_datawarehouse.close()

    return


def handling_meals_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    valid_meals = ['BB', 'HB', 'FB', 'SC', 'SCD']
    df['meal'] = df['meal'].apply(lambda x: x if x in valid_meals else "Undefined")

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for meals column by the value - 'Undefined'")
    print("\n")
    db_datawarehouse.close()

    return


def handling_marketsegment_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    valid_market_segments = ['Direct', 'Corporate', 'Online TA', 'Offline TA/TO', 'Complementary', 'Groups']
    df['market_segment'] = df['market_segment'].apply(lambda x: x if x in valid_market_segments else "Undefined")

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for market segment column by the value - 'Undefined'")
    print("\n")
    db_datawarehouse.close()

    return


def handling_distchannels_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    valid_distribution_channels = ['Direct', 'Corporate', 'TA/TO', 'GDS']
    df['distribution_channel'] = df['distribution_channel'].apply(lambda x: x if x in valid_distribution_channels else "Undefined")

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for distribution channel column by the value - 'Undefined'")
    print("\n")
    db_datawarehouse.close()

    return


def handling_reappeardguest_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['is_repeated_guest'] = df['is_repeated_guest'].apply(lambda x: 1 if x == 1 else 0)

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for reappeared guest column by the value - 0")
    print("\n")
    db_datawarehouse.close()

    return


def handling_deposittype_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['deposit_type'] = df['deposit_type'].apply(lambda x: x if x in ['No Deposit', 'Non Refund', 'Refundable'] else "No Deposit")

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for deposit type column by the value - 'No Deposit'")
    print("\n")
    db_datawarehouse.close()

    return


def handling_customertype_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['customer_type'] = df['customer_type'].apply(lambda x: x if x in ['Transient', 'Contract', 'Group', 'Transient-Party'] else "Transient")

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for customer type column by the value - 'Transient'")
    print("\n")
    db_datawarehouse.close()

    return


def handling_reqcarparking_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['required_car_parking_spaces'] = df['required_car_parking_spaces'].apply(lambda x: x if x in [0, 1] else 0)

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for required car parking spaces column by the value - 0")
    print("\n")
    db_datawarehouse.close()

    return


def handling_totalspecialrequests_col_nulls():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['total_of_special_requests'] = df['total_of_special_requests'].apply(lambda x: x if x >= 0 else 0)

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("replaced all the null values for total special requests column by the value - 0")
    print("\n")
    db_datawarehouse.close()

    return


def create_bookingdate_col():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['arrival_date'] = pd.to_datetime(df['arrival_date_year'].astype(str) + '-' +
                                        df['arrival_date_month'] + '-' +
                                        df['arrival_date_day_of_month'].astype(str), format='%Y-%B-%d')
    df['booking_date'] = (df['arrival_date'] - pd.to_timedelta(df['lead_time'], unit='d')).dt.date

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("created a new column - booking date")
    print("\n")
    db_datawarehouse.close()

    return


def create_season_col():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    seasons = {
    'January': 'Winter', 'February': 'Winter', 'March': 'Spring', 
    'April': 'Spring', 'May': 'Spring', 'June': 'Summer', 
    'July': 'Summer', 'August': 'Summer', 'September': 'Fall', 
    'October': 'Fall', 'November': 'Fall', 'December': 'Winter'
    }
    df['season'] = df['arrival_date_month'].map(seasons)

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print(df.columns)
    print("created a new column - booking date")
    print("\n")
    db_datawarehouse.close()

    return


def data_profiling_after():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    data_report = data_quality_report(df)
    data_report.to_sql(name='data_profiling_after', con=db_datawarehouse, if_exists='replace')
    print("=======================")
    print("Initial data profiling done - check data_profiling_after table in the database hotel_datawarehouse")
    print("\n")
    db_datawarehouse.close()
    print(data_report)

    return


def handling_target_feature():
    engine = create_engine('mysql://root:password@localhost:3306/hotel_datawarehouse', echo=False)
    db_datawarehouse = engine.connect()

    str_sql_bookings = 'SELECT * FROM hotel_bookings;'
    df = pd.read_sql(sql=str_sql_bookings, con=db_datawarehouse)

    df['booking_date'] = pd.to_datetime(df['booking_date'])
    df['reservation_status_date'] = pd.to_datetime(df['reservation_status_date'])
    current_period = int(Variable.get('current_period', default_var=0)) - 1
    current_date = get_current_date(current_period)
    cancelled_rows = df[(df['booking_date'] == pd.Timestamp(current_date)) & (df['booking_date'] == df['reservation_status_date']) & (df['reservation_status'] == 'Canceled')]
    print(len(cancelled_rows))
    print(df['is_canceled'].value_counts())
    for _,row in df.iterrows():
        if row['booking_date'] == pd.Timestamp(current_date):
            if row['reservation_status'] == 'Canceled':
                row['is_canceled'] = 1
            else:
                row['is_canceled'] = 0
    print(df['is_canceled'].value_counts())

    df.to_sql(name='hotel_bookings', con=db_datawarehouse, if_exists='replace', index=False)
    print("=======================")
    print(df.shape)
    print("handled the target column with a custom rule to simulate real-world operations")
    print("\n")
    db_datawarehouse.close()

    return


def check_and_update_current_period():
    cp = int(Variable.get('current_period', default_var=0))
    current_period = int(Variable.get('current_period', default_var=0))
    updated_period = int(current_period) + 1
    Variable.set('current_period', updated_period)
    print(f"Updated current_period to {updated_period}")

    if cp == 0:
        return 'clean_datawarehouse'
    else:
        return 'initial_load_current_data'


with DAG(
    'gp_dataops',
    default_args={
        'depends_on_past': False,
        'email': ['685@doonschool.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Group Project Data Ops',

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
    clean_datawarehouse = PythonOperator(
        task_id='clean_datawarehouse',
        python_callable=clean_datawarehouse
        # op_kwargs={'arg1': 1, 'arg2': 2}
    )

    create_datawarehouse_and_upload_hist = PythonOperator(
        task_id='create_datawarehouse_and_upload_hist',
        python_callable=create_datawarehouse_and_upload_hist
    )

    initial_load_current_data = PythonOperator(
        task_id='initial_load_current_data',
        python_callable=initial_load_current_data
    )

    data_profiling_before = PythonOperator(
        task_id='data_profiling_before',
        python_callable=data_profiling_before
    )

    kpi_threshold_check = PythonOperator(
        task_id='kpi_threshold_check',
        python_callable=kpi_threshold_check
    )

    replace_negative_values = PythonOperator(
        task_id='replace_negative_values',
        python_callable=replace_negative_values
    )

    handling_hotel_col_nulls = PythonOperator(
        task_id='handling_hotel_col_nulls',
        python_callable=handling_hotel_col_nulls
    )

    handling_children_and_babies_cols_nulls = PythonOperator(
        task_id='handling_children_and_babies_cols_nulls',
        python_callable=handling_children_and_babies_cols_nulls
    )

    handling_meals_col_nulls = PythonOperator(
        task_id='handling_meals_col_nulls',
        python_callable=handling_meals_col_nulls
    )

    handling_deposittype_col_nulls = PythonOperator(
        task_id='handling_deposittype_col_nulls',
        python_callable=handling_deposittype_col_nulls
    )

    handling_marketsegment_col_nulls = PythonOperator(
        task_id='handling_marketsegment_col_nulls',
        python_callable=handling_marketsegment_col_nulls
    )

    handling_distchannels_col_nulls = PythonOperator(
        task_id='handling_distchannels_col_nulls',
        python_callable=handling_distchannels_col_nulls
    )

    handling_reappeardguest_col_nulls = PythonOperator(
        task_id='handling_reappeardguest_col_nulls',
        python_callable=handling_reappeardguest_col_nulls
    )

    handling_customertype_col_nulls = PythonOperator(
        task_id='handling_customertype_col_nulls',
        python_callable=handling_customertype_col_nulls
    )

    handling_reqcarparking_col_nulls = PythonOperator(
        task_id='handling_reqcarparking_col_nulls',
        python_callable=handling_reqcarparking_col_nulls
    )

    handling_totalspecialrequests_col_nulls = PythonOperator(
        task_id='handling_totalspecialrequests_col_nulls',
        python_callable=handling_totalspecialrequests_col_nulls
    )

    create_bookingdate_col = PythonOperator(
        task_id='create_bookingdate_col',
        python_callable=create_bookingdate_col
    )

    create_season_col = PythonOperator(
        task_id='create_season_col',
        python_callable=create_season_col
    )

    data_profiling_after = PythonOperator(
        task_id='data_profiling_after',
        python_callable=data_profiling_after
    )

    handling_target_feature = PythonOperator(
        task_id='handling_target_feature',
        python_callable=handling_target_feature
    )

    branch_task_check_update_current_period = BranchPythonOperator(
        task_id='check_and_update_current_period',
        provide_context=True,
        python_callable=check_and_update_current_period
    )

    start = DummyOperator(task_id='start')

    start >> branch_task_check_update_current_period >> [clean_datawarehouse, initial_load_current_data]
    clean_datawarehouse >> create_datawarehouse_and_upload_hist
    initial_load_current_data >> data_profiling_before >> kpi_threshold_check >> replace_negative_values >> handling_hotel_col_nulls >> handling_children_and_babies_cols_nulls
    handling_children_and_babies_cols_nulls >> handling_meals_col_nulls >> handling_marketsegment_col_nulls >> handling_distchannels_col_nulls >> handling_reappeardguest_col_nulls
    handling_reappeardguest_col_nulls >> handling_deposittype_col_nulls >> handling_customertype_col_nulls >> handling_reqcarparking_col_nulls >> handling_totalspecialrequests_col_nulls
    handling_totalspecialrequests_col_nulls >> create_bookingdate_col >> create_season_col >> handling_target_feature >> data_profiling_after


