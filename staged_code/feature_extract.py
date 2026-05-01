def run(input_data):
    data = input_data if isinstance(input_data, dict) else {"value": input_data}
    return {**data, "features": [0.1, 0.2, 0.3, 0.4, 0.5], "feature_count": 5}
