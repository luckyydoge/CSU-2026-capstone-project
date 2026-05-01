def run(input_data):
    data = input_data if isinstance(input_data, dict) else {"value": input_data}
    return {
        "result": {
            "label": data.get("label", "unknown"),
            "confidence": data.get("confidence", 0),
            "features_used": len(data.get("features", []))
        },
        "metadata": {"pipeline": "full", "completed": True}
    }
