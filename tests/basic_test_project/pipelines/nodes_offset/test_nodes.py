import random

def create_offsets(signal: dict, seed: int)->tuple:
    random.seed(seed)
    offesets = []
    for i in range(len(signal["time"])):
        offesets.append(random.randint(0, 100))
    return offesets

def update_signal(signal: dict, offsets: list):
    for i in range(len(signal["signal"])):
        signal["signal"][i] += offsets[i]
    return signal