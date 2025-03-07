import random

def create_offsets(signal: dict, seed: int)->tuple:
    random.seed(seed)
    offesets = []
    for i in range(len(signal["time"])):
        offesets.append(random.randint(0, 100))
    return tuple(offesets)

def update_signal(signal: dict, offsets: list):
    for i in range(len(signal["signal"])):
        signal["signal"][i] += offsets[i]
    return signal

def split_signal(signal: dict) -> tuple[dict, dict]:
    signal1 = {"time": signal["time"], "signal": signal["signal"][:len(signal["signal"])//2]}
    signal2 = {"time": signal["time"], "signal": signal["signal"][len(signal["signal"])//2:]}
    return signal1, signal2

def substract_signals(signal1: dict, signal2: dict) -> dict:
    signal = {"time": signal1["time"], "signal": [x-y for x, y in zip(signal1["signal"], signal2["signal"])]}
    return signal
