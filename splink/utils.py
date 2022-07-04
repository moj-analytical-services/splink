import numpy as np
import json


class NumpyEncoder(json.JSONEncoder):
    """
    Used to correctly encode numpy columns within a pd dataframe
    when dumping it to json. Without this, json.dumps errors if
    given an a column of class int32, int64 or np.array.
    
    Thanks to:
    https://github.com/mpld3/mpld3/issues/434#issuecomment-340255689
    """

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)
