import requests

def check_api_health():
    url = "https://hotel-cancellation-ans-5feb2a4e271c.herokuapp.com/"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print("API is live:", response.json())
        else:
            print("API check failed with status code:", response.status_code)
    except Exception as e:
        print("Error during API check:", str(e))

check_api_health()