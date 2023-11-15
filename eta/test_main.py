import pandas as pd
import time
import sys
import warnings
import asyncio
warnings.filterwarnings("ignore")

# ASYNC
async def test_async_prediction(gps: pd.DataFrame):
    start_time = time.time()
    from bus_eta_application import BusETAApplication

    eta = BusETAApplication('assets/')
    print("Time for preparation:", time.time() - start_time)

    result = await eta.predict_async(gps)
    print(f"total bus code: {len(result)}")
    print(result["TJ3845"])
    print("Time for prediction:", time.time() - start_time)

def run_async_prediction(gps: pd.DataFrame):
    asyncio.run(test_async_prediction(gps))

# SYNC
def test_sync_prediction(gps: pd.DataFrame):
    start_time = time.time()
    from bus_eta_application import BusETAApplication
    eta = BusETAApplication('assets/')
 
    print("Time for preparation:", time.time() - start_time)

    result = eta.predict(gps, debug=True)
    print(f"total bus_code: {len(result)}")
    print(result[list(result.keys())[0]])
    print("Time for prediction:", time.time() - start_time)

if __name__ == "__main__":
    gps = pd.read_csv('assets/debug_input.csv')

    if len(sys.argv) < 2:
        print("Please specify the method (sync/async)")
        sys.exit(1)
    else:
        method = sys.argv[1]
        if method == "sync":
            test_sync_prediction(gps)
        elif method == "async":
            run_async_prediction(gps)

            
