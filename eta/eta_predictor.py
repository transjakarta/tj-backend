import asyncio
import pandas as pd
import numpy as np
from xgboost import XGBRegressor

# This class handles the ETA prediction logic.
# Useful function: predict_eta
class ETAPredictor:
    def __init__(self, model, map):
        self.model = model
        self.map = map

    async def _process_row_async(self, gps, row, last_stop, preds):
        modified_rows = self._generate_modified_rows(gps, row, last_stop)
        to_predict = pd.DataFrame(modified_rows)

        # Preparing the DataFrame for prediction
        next_stops = to_predict['next_stop']
        to_predict = self._prepare_for_prediction(to_predict)

        # Make predictions asynchronously
        pred = self.model.predict(to_predict)

        return self._accumulate_predictions(pred, next_stops, preds)

    async def predict_eta_async(self, gps):
        gps = gps.reset_index(drop=True)

        preds = dict()
        last_stop = gps.loc[0, 'prev_stop']

        # Asynchronously process each row
        tasks = [self._process_row_async(gps, row, last_stop, preds) for _, row in gps.iterrows()]
        preds = await asyncio.gather(*tasks)

        # Post-process the predictions
        preds = self._finalize_predictions(preds[0], len(gps))
        return preds
    
    def predict_eta(self, gps):
        """
        Predicts the estimated time of arrival (ETA) for each stop.

        Args:
        gps (pd.DataFrame): DataFrame containing GPS data.

        Returns:
        dict: A dictionary with stops as keys and predicted ETAs as values.
        """
        gps = gps.reset_index(drop=True)

        preds = dict()
        last_stop = gps.loc[0, 'prev_stop']

        for _, row in gps.iterrows():
            modified_rows = self._generate_modified_rows(gps, row, last_stop)
            to_predict = pd.DataFrame(modified_rows)

            # Preparing the DataFrame for prediction
            next_stops = to_predict['next_stop']
            to_predict = self._prepare_for_prediction(to_predict)

            # Make predictions
            pred = self.model.predict(to_predict)

            preds = self._accumulate_predictions(pred, next_stops, preds)

        # Post-process the predictions
        preds = self._finalize_predictions(preds, len(gps))

        return preds

    def _generate_modified_rows(self, gps, row, last_stop):
        """
        Generates modified rows for prediction based on the current GPS data row.

        Args:
        gps (pd.DataFrame): DataFrame containing GPS data.
        row (pd.Series): A single row from the GPS DataFrame.
        last_stop (str): ID of the last stop.

        Returns:
        list: A list of modified rows for prediction.
        """
        modified_rows = []
        trip = row['trip_id']
        real = row.copy()

        for j in range(3):
            stops = self._get_stops(self.map[trip])
            n_stop = len(stops)
            idx = self._get_start_index(stops, row['next_stop'], j)

            for i in range(idx, n_stop):
                if j == 0 and i == 0:
                    modified_rows.append(real)
                    continue

                temp = self._create_temp_row(real, stops, i, trip, last_stop)
                if temp is None:  # Break if the next stop is the last stop
                    return modified_rows

                modified_rows.append(temp)

            if not self._update_trip(trip):
                break
            else:
                trip = self._update_trip(trip)

        return modified_rows

    def _get_stops(self, trip_map):
        """
        Retrieves stops from the trip map.

        Args:
        trip_map (dict): Trip map containing status of stops.

        Returns:
        list: List of stops.
        """
        return [(id, index) for index, id in enumerate(trip_map['status']) if id != '.']

    def _get_start_index(self, stops, next_stop, iteration):
        """
        Gets the starting index for iterating over stops.

        Args:
        stops (list): List of stops.
        next_stop (str): ID of the next stop.
        iteration (int): Current iteration number.

        Returns:
        int: The starting index.
        """
        return [id for id, _ in stops].index(next_stop) if iteration == 0 else 1

    def _create_temp_row(self, real, stops, index, trip, last_stop):
        """
        Creates a temporary row for prediction.

        Args:
        real (pd.Series): A copy of the current row from the GPS DataFrame.
        stops (list): List of stops.
        index (int): Current index in the stops list.
        trip (str): Current trip ID.
        last_stop (str): ID of the last stop.

        Returns:
        pd.Series or None: A temporary row for prediction or None if the next stop is the last stop.
        """
        idx_cur_in_shape = stops[index - 1][1]
        cur_stop = stops[index - 1][0]
        next_stop = stops[index][0]

        if next_stop == last_stop:
            return None

        temp = real.copy()
        temp['prev_stop'] = cur_stop
        temp['next_stop'] = next_stop
        temp['latitude'] = self.map[trip]['shape'][idx_cur_in_shape][0]
        temp['longitude'] = self.map[trip]['shape'][idx_cur_in_shape][1]
        temp['next_stop_dist'] = self.map[trip]['jarak'][cur_stop][1]

        return temp

    def _update_trip(self, trip):
        """
        Updates the trip based on the pair information in the map.

        Args:
        trip (str): Current trip ID.

        Returns:
        bool: True if the trip is updated, False otherwise.
        """
        if self.map[trip]['pair']:
            return self.map[trip]['pair']
        return False

    def _prepare_for_prediction(self, to_predict):
        """
        Prepares the DataFrame for prediction by dropping unnecessary columns and mapping categorical values.

        Args:
        to_predict (pd.DataFrame): DataFrame to be prepared for prediction.

        Returns:
        pd.DataFrame: The prepared DataFrame.
        """
        to_predict = to_predict.drop(['bus_code', 'gpsdatetime', 'is_new', 'distance_route', 'following_route',
                                      'trip_shape', 'next_stop', 'prev_stop', 'next_stop_seq', 'prev_stop_seq',
                                      'trip_id'], axis=1)
        to_predict["koridor"] = to_predict["koridor"].map({'4B': 0, '9H': 1, 'D21': 2})
        to_predict = to_predict.apply(pd.to_numeric)
        return to_predict.reindex(columns=self.model.get_booster().feature_names)

    def _accumulate_predictions(self, pred, next_stops, preds):
        """
        Accumulates the predictions for each stop.

        Args:
        pred (np.array): Array of predictions.
        next_stops (pd.Series): Series of next stops.
        preds (dict): Dictionary to accumulate predictions.
        """
        sum = 0
        for i, stop in enumerate(next_stops):
            if stop not in preds:
                preds[stop] = []
            sum += pred[i]
            preds[stop].append(sum)
        return preds

    def _finalize_predictions(self, preds, n):
        """
        Finalizes the predictions by calculating the 25th percentile.

        Args:
        preds (dict): Dictionary containing accumulated predictions.
        n (int): Number of GPS data points.
        """
        to_pop = [key for key, val in preds.items() if len(val) != n]
        for key in to_pop:
            preds.pop(key)

        for key in preds:
            preds[key] = np.percentile(preds[key], 25)

        return preds