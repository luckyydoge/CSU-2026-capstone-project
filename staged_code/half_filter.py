
def run(data):
    if isinstance(data, list):
        return data[:len(data)//2]
    return data
