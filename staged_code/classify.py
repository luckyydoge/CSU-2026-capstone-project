def run(input_data):
    data = input_data if isinstance(input_data, dict) else {"value": input_data}
    features = data.get("features", [])
    label = "cat" if len(features) > 3 else "dog"
    confidence = 0.95 if len(features) > 3 else 0.60
    return {**data, "label": label, "confidence": confidence}
