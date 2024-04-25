import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline
import joblib
import xgboost

from xgboost import XGBClassifier

feature_names = []

def train_and_evaluate_model(hotel_bookings_train_data):
    global feature_names  # Declare feature_names as a global variable

    hotel_bookings_train_data.rename(columns={'Unnamed: 0': 'booking_id'}, inplace=True)

    # Custom preprocessing
    hotel_bookings_train_data = hotel_bookings_train_data.copy()
    hotel_bookings_train_data = hotel_bookings_train_data.drop(hotel_bookings_train_data.columns[0], axis=1)
    hotel_bookings_train_data.fillna(-1, inplace=True)
    
    filter = (hotel_bookings_train_data['children'] == 0) & (hotel_bookings_train_data['adults'] == 0) & (hotel_bookings_train_data['babies'] == 0)
    hotel_bookings_train_data = hotel_bookings_train_data[~filter]
    
    useless_col = ['days_in_waiting_list', 'arrival_date_year', 'assigned_room_type', 'booking_changes', 'reservation_status', 'country', 'days_in_waiting_list']
    hotel_bookings_train_data.drop(useless_col, axis=1, inplace=True)
    
    hotel_bookings_train_data["arrival_date"] = pd.to_datetime(hotel_bookings_train_data["arrival_date"])
    hotel_bookings_train_data["booking_date"] = pd.to_datetime(hotel_bookings_train_data["booking_date"])
    
    cat_cols = [col for col in hotel_bookings_train_data.columns if hotel_bookings_train_data[col].dtype == 'O']
    cat_df = hotel_bookings_train_data[cat_cols]
    if 'reservation_status_date' in cat_df:
        cat_df['reservation_status_date'] = pd.to_datetime(cat_df['reservation_status_date'])
        cat_df['year'] = cat_df['reservation_status_date'].dt.year
        cat_df['month'] = cat_df['reservation_status_date'].dt.month
        cat_df['day'] = cat_df['reservation_status_date'].dt.day
        cat_df.drop(['reservation_status_date'], axis=1, inplace=True)
    
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
        if col in cat_df:
            cat_df[col] = cat_df[col].map(mapping)
            cat_df[col] = cat_df[col].fillna(-1)
    
    num_df = hotel_bookings_train_data.select_dtypes(include=['int64', 'float64'])
    for col in ['lead_time', 'arrival_date_week_number', 'arrival_date_day_of_month', 'agent', 'adr']:
        if col in num_df:
            num_df[col] = np.log(num_df[col] + 1)
    
    cat_encoder = OneHotEncoder()
    cat_df_encoded = cat_encoder.fit_transform(cat_df)
    cat_df_encoded = pd.DataFrame(cat_df_encoded.toarray(), columns=cat_encoder.get_feature_names_out(cat_df.columns))
    
    hotel_bookings_train_data = pd.concat([cat_df_encoded, num_df], axis=1)
    hotel_bookings_train_data.fillna(0, inplace=True)

    # Store the feature names in the global variable feature_names
    feature_names = hotel_bookings_train_data.columns.tolist()

    # Splitting into features and target
    X = hotel_bookings_train_data.drop('is_canceled', axis=1)
    y = hotel_bookings_train_data['is_canceled']
    
    # Splitting into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.30, random_state=42)

    # Model training
    model_pipe = Pipeline(steps=[
        ('scaler', StandardScaler()),  # Standardizing numerical features
        ('classifier', XGBClassifier(learning_rate=0.121333233516, max_depth=3, n_estimators=300))  # XGBoostClassifier
    ])

    # Fitting the model
    model_pipe.fit(X_train, y_train)

    # Predictions
    y_pred = model_pipe.predict(X_test)
    y_prob = model_pipe.predict_proba(X_test)

    print(feature_names)  # Print feature names to verify

    # Classification report
    class_report = classification_report(y_test, y_pred)

    # Save the trained model to a pickle file
    joblib.dump(model_pipe, "Retrained4301model.pkl")

    return model_pipe, y_prob, class_report


df = pd.read_csv('hotel_bookings_train_data.csv', delimiter=';')
df['arrival_date'] = pd.to_datetime(df['arrival_date'], format='%d/%m/%Y')
df['booking_date'] = pd.to_datetime(df['booking_date'], format='%d/%m/%Y')
df['arrival_date'] = pd.to_datetime(df['arrival_date'], format='%d/%m/%Y')
df['booking_date'] = pd.to_datetime(df['booking_date'], format='%d/%m/%Y')
df['reservation_status_date'] = pd.to_datetime(df['reservation_status_date'], format='%d/%m/%Y')

# Split your dataset into training and testing sets
Traindf = df.iloc[:-50]
Testdf = df.iloc[-50:]

# Train the model and get the model, probability predictions, and classification report
trained_model, y_prob, class_report = train_and_evaluate_model(Traindf)
print("Classification Report:\n", class_report)
