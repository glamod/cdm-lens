DATA_VERSIONS = {
    "v1": "lite_2_0",
    "v2": "lite_2_0"
}

DEFAULT_VERSION = "v2"

def validate_data_version(v):
    v = v or DEFAULT_VERSION
    v = str(v).lower()
 
    if v not in DATA_VERSIONS:
        raise ValueError(f"Data version must be one of: {list(DATA_VERSIONS)}")

    return v

