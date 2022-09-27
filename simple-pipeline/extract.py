import json

import requests


if __name__ == "__main__":
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()
    print(data)

    try:
        with open("dog.json", "r") as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = {
            "data": []
        }

    existing_data["data"].append(data)

    with open("dog.json", "w") as f:
        json.dump(existing_data, f)