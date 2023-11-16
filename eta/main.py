import pandas as pd
import warnings
import asyncio
from .bus_eta_application import BusETAApplication
warnings.filterwarnings("ignore")

# ASYNC
async def async_prediction(gps: pd.DataFrame):
    eta = BusETAApplication('assets/')
    result = await eta.predict_async(gps)
    return result

def run_async_prediction(gps: pd.DataFrame):
    asyncio.run(async_prediction(gps))

# SYNC
def sync_prediction(gps: pd.DataFrame):
    eta = BusETAApplication('assets/')
    result = eta.predict_debug(gps)
    return result

            
