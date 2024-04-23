#Perform stress test on the model API by invoking it twice

import requests
import json

def simulate_api_load():
    url = "https://ml-hotel-cancellation-prod-d6b6251be054.herokuapp.com/predict"
    payload = {
        "data": {
            "hotel": 1,  
            "meal": 0,
            "market_segment": 2,
            "distribution_channel": 2,
            "reserved_room_type": 1,
            "deposit_type": 0,
            "customer_type": 0,
            "year": 2017,
            "month": 7,
            "day": 15,
            "lead_time": 10,
            "arrival_date_week_number": 29,
            "arrival_date_day_of_month": 15,
            "stays_in_weekend_nights": 2,
            "stays_in_week_nights": 5,
            "adults": 2,
            "children": 1,
            "babies": 0,
            "is_repeated_guest": 0,
            "previous_cancellations": 0,
            "previous_bookings_not_canceled": 0,
            "agent": 3,
            "adr": 150.0,
            "required_car_parking_spaces": 0,
            "total_of_special_requests": 3
        }
    }

    headers = {'Content-Type': 'application/json'}
    for _ in range(2):
        response = requests.post(url, json=payload, headers=headers)
        print("Response status:", response.status_code, "; Response data:", response.json())

simulate_api_load()