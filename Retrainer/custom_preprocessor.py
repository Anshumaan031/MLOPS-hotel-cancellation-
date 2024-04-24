from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np

class CustomPreprocessor(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        # Fit should handle setting up any parameters/information needed for transformation
        return self
    
    def transform(self, X, y=None):
        X = X.copy()  # To avoid changes to the original data
        
        # Preprocessing steps as specified
        X = X.drop(X.columns[0], axis=1)
        X.fillna(-1, inplace=True)
        filter = (X['children'] == 0) & (X['adults'] == 0) & (X['babies'] == 0)
        X = X[~filter]
        useless_col = ['days_in_waiting_list', 'arrival_date_year', 'assigned_room_type', 'booking_changes',
                       'reservation_status', 'country', 'days_in_waiting_list']
        X.drop(useless_col, axis=1, inplace=True)
        X["arrival_date"] = pd.to_datetime(X["arrival_date"])
        X["booking_date"] = pd.to_datetime(X["booking_date"])

        cat_cols = [col for col in X.columns if X[col].dtype == 'O']
        cat_df = X[cat_cols]

        if 'reservation_status_date' in cat_df:
            cat_df['reservation_status_date'] = pd.to_datetime(cat_df['reservation_status_date'])
            cat_df['year'] = cat_df['reservation_status_date'].dt.year
            cat_df['month'] = cat_df['reservation_status_date'].dt.month
            cat_df['day'] = cat_df['reservation_status_date'].dt.day
            cat_df.drop(['reservation_status_date', 'arrival_date_month'], axis=1, inplace=True)
        
        # Encoding categorical variables
        mappings = {
            'hotel': {'Resort Hotel': 0, 'City Hotel': 1},
            'meal': {'BB': 0, 'FB': 1, 'HB': 2, 'SC': 3, 'Undefined': 4},
            'market_segment': {'Direct': 0, 'Corporate': 1, 'Online TA': 2, 'Offline TA/TO': 3,
                               'Complementary': 4, 'Groups': 5, 'Undefined': 6, 'Aviation': 7},
            'distribution_channel': {'Direct': 0, 'Corporate': 1, 'TA/TO': 2, 'Undefined': 3,
                                     'GDS': 4},
            'reserved_room_type': {'C': 0, 'A': 1, 'D': 2, 'E': 3, 'G': 4, 'F': 5, 'H': 6,
                                   'L': 7, 'B': 8},
            'deposit_type': {'No Deposit': 0, 'Refundable': 1, 'Non Refund': 3},
            'customer_type': {'Transient': 0, 'Contract': 1, 'Transient-Party': 2, 'Group': 3},
            'year': {2015: 0, 2014: 1, 2016: 2, 2017: 3}
        }

        for col, mapping in mappings.items():
            if col in cat_df:
                cat_df[col] = cat_df[col].map(mapping)
                cat_df[col] = cat_df[col].fillna(-1)

        # Continue with other encoding as specified
        # Note: This is simplified for brevity. Please include all your mappings.

        num_df = X.select_dtypes(include=['int64', 'float64'])
        #num_df.drop('is_canceled', axis=1, inplace=True)

        # Log transformation
        for col in ['lead_time', 'arrival_date_week_number', 'arrival_date_day_of_month', 'agent', 'adr']:
            if col in num_df:
                num_df[col] = np.log(num_df[col] + 1)

        # Merge categorical and numerical dataframes
        X_transformed = pd.concat([cat_df, num_df], axis=1)
        X_transformed.fillna(0, inplace=True)
        
        return X_transformed
