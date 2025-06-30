

def get_key_from_value(d, val): #https://note.nkmk.me/en/python-dict-get-key-from-value/
    keys = [k for k, v in d.items() if v == val]
    if keys:
        return keys[0]
    return None
