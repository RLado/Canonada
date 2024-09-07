import random
import uuid
import os
import json

def gen(num_signals: int, sig_len: int):
    """
    Generate random time series signals and save them in json files 
    """
    if not os.path.exists("data/raw_signals"):
        os.makedirs("data/raw_signals")
    else: 
        for file in os.listdir("data/raw_signals"):
            os.remove(os.path.join("data/raw_signals", file))
    
    # Also generate a dictionary for the resulting offset signals and others
    if not os.path.exists("data/offset_signals"):
        os.makedirs("data/offset_signals")
    else:
        for file in os.listdir("data/offset_signals"):
            os.remove(os.path.join("data/offset_signals", file))
    
    if not os.path.exists("data/substracted_signals"):
        os.makedirs("data/substracted_signals")
    else:
        for file in os.listdir("data/substracted_signals"):
            os.remove(os.path.join("data/substracted_signals", file))

    if not os.path.exists("data/split_signals1"):
        os.makedirs("data/split_signals1")
    else:
        for file in os.listdir("data/split_signals1"):
            os.remove(os.path.join("data/split_signals1", file))
    if not os.path.exists("data/split_signals2"):
        os.makedirs("data/split_signals2")
    else:
        for file in os.listdir("data/split_signals2"):
            os.remove(os.path.join("data/split_signals2", file))    
    
    # Generate signals
    for i in range(num_signals):
        time = [i for i in range(sig_len)]
        signal = [random.random() for _ in range(sig_len)]
        data = {
            "id": str(uuid.uuid4()),
            "time": time,
            "signal": signal
        }
        with open(os.path.join("data/raw_signals", f"signal_{i}.json"), "w") as f:
            json.dump(data, f)

