import requests
import json

def test_cases():
    url = "https://ml-hotel-cancellation-prod-d6b6251be054.herokuapp.com/predict"
    # Valid case
    valid_payload = {
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

    invalid_payload = {
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
            "adr": 150.0,
            "required_car_parking_spaces": 0,
            "total_of_special_requests": 3
        }
    }

    headers = {'Content-Type': 'application/json'}
    
    # Testing valid case
    response = requests.post(url, json=valid_payload, headers=headers)
    print("Valid case - Response status:", response.status_code, "; Response data:", response.json())
    
    # Testing invalid case
    response = requests.post(url, json=invalid_payload, headers=headers)
    if response.status_code != 200:
        print("Invalid case - Expected failure, Response status:", response.status_code, "; Error message:", response.text)

test_cases()