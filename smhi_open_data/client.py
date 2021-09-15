import json
from typing import List, Dict, Any, Union

import requests
from tenacity import retry, stop_after_attempt, wait_random

import pandas as pd

from smhi_open_data.smhi_open_data.enums import Parameter
from smhi_open_data.smhi_open_data.utils import (
    try_parse_float,
    distance,
    json_to_dataframe,
    format_archived_dataframe,
)


class SMHIOpenDataClient:
    _base_url = "https://opendata-download-metobs.smhi.se"

    def __init__(self, version: str = "1.0"):
        self.version = version

    @property
    def base_url(self):
        return f"{self._base_url}/api/version/{self.version}"

    @retry(stop=stop_after_attempt(10), wait=wait_random(min=0.01, max=0.10))
    def _query(self, service: str, params: Dict[str, Any] = None, **kwargs):
        # Create a session
        s = requests.Session()

        # Create adapters with the retry logic for each
        http = requests.adapters.HTTPAdapter(max_retries=30)
        https = requests.adapters.HTTPAdapter(max_retries=30)

        # Replace the session's original adapters
        s.mount("http://", http)
        s.mount("https://", https)

        # Start using the session
        res = s.get(url=f"{self.base_url}/{service}", stream=True)

        # if res.encoding is None:
        #     res.encoding = "utf-8"

        # data = []
        # for line in res.iter_lines(decode_unicode=True):
        #     if line:
        #         data.append(line)
        # return json.loads("".join(data))
        return res.json()

    @retry(stop=stop_after_attempt(10), wait=wait_random(min=0.01, max=0.10))
    def _query_csv(self, service: str):
        # download the csv data
        df = pd.read_csv(
            filepath_or_buffer=f"{self.base_url}/{service}",
            skiprows=9,
            delimiter=";",
            low_memory=False,
        )
        return df

    def get_parameter_stations(self, parameter: Parameter) -> List[Dict[str, Any]]:
        """Get parameter stations.

        Returns:
            List[Dict[str, Any]]: List of SMHI stations where parameter is available.
        """
        res = self._query(service=f"parameter/{parameter.value}.json")
        return res["station"]

    def get_station_parameters(
        self, station_id: int, parameter_set: List[Parameter] = list(Parameter)
    ) -> List[Parameter]:
        parameters = set([])
        for parameter in parameter_set:
            for station in self.get_parameter_stations(parameter=parameter):
                if station["id"] == station_id:
                    parameters.add(parameter)
        return list(parameters)

    def get_stations(self) -> List[Dict[str, Any]]:
        """Get SMHI stations. NOTE: Performs multiple http requests.

        NOTE: This performs multiple requests and might take long time to run.

        Returns:
            List[Dict[str, Any]]: List of SMHI stations.
        """
        stations, station_ids = [], set([])
        for param in Parameter:
            stations = self.get_parameter_stations(parameter=param)
            for station in stations:
                if station["id"] not in station_ids:
                    station_ids.add(station["id"])
                    stations.append(station)
        return stations

    def get_latest_observations(self, parameter: Parameter) -> List[Dict[str, Any]]:
        """Get latest raw SMHI observations from available stations.

        Args:
            parameter_id (Parameter): Returns observations for a specific parameter.

        Returns:
            List[Dict[str, Any]]: List of raw SMHI observations from available stations.
        """
        # Check if parameter has station set
        res = self._query(service=f"parameter/{parameter.value}/station-set.json")
        station_set = res["stationSet"]

        if station_set is None:
            # TODO: Get values from stations instead
            raise NotImplementedError(f"Not implemented for parameter: {parameter}")

        # Check if station set has any periods
        res = self._query(service=f"parameter/{parameter.value}/station-set/all.json")
        periods = res["period"]
        if periods is None or len(periods) == 0:
            raise NotImplementedError(f"Not implemented for parameter: {parameter}")

        # Get period key
        period_key = periods[0]["key"]
        if period_key != "latest-hour":
            raise NotImplementedError(
                f"Not implemented for parameter: {parameter} and period: {period_key}"
            )

        # Get period data
        res = self._query(
            service=f"parameter/{parameter.value}/station-set/all/period/latest-hour/data.json"
        )

        values = []
        for x in res["station"]:
            value_list = x["value"]
            if value_list is None:
                continue

            for value in value_list:
                values.append(
                    {
                        "parameter_id": parameter.value,
                        "timestamp": value["date"],
                        "value": try_parse_float(value["value"]),
                        "station": x["key"],
                    }
                )

        return values

    def list_parameters(self) -> List[Dict[str, Union[str, Parameter]]]:
        """List available observation parameters.

        Returns:
            List[Dict[str, Union[str, Parameter]]]: List of dictionaries
                containing information about each available observations
                parameter.
        """
        return [
            {"name": parameter.name, "value": parameter.value, "enum": parameter}
            for parameter in Parameter
        ]

    @staticmethod
    def get_parameter(parameter_id: int) -> Parameter:
        """Get parameter enum from SMHI parameter id.

        Args:
            parameter_id (int): Parameter id found on SMHI API documentation.

        Returns:
            Parameter: Parameter enum object.
        """
        return Parameter(parameter_id)

    def get_closest_station(
        self, latitude: float, longitude: float
    ) -> List[Dict[str, Any]]:
        """Get closest weather station from given coordinates.

        Args:
            latitude (float): Latitude coordinate.
            longitude (float): Longitude coordinate.

        Returns:
            List[Dict[str, Any]]: Closest weather station.
        """
        stations = self.get_stations()
        closest_station, closests_dist = None, 1e10
        for station in stations:
            lat, lon = station.get("latitude"), station.get("longitude")
            if lat is None or lon is None:
                continue

            # Calculate distance
            dist = distance(lat1=latitude, lon1=longitude, lat2=lat, lon2=lon)

            if dist < closests_dist:
                closests_dist, closest_station = dist, station
        return closest_station

    def get_corrected_data(self, parameter: Parameter, station_id: int) -> pd.DataFrame:
        """Get archived corrected data for specified parameter and station.
        """

        df = self._query_csv(
            service=f"parameter/{parameter.value}/station/{station_id}/period/corrected-archive/data.csv"
        )
        df = format_archived_dataframe(df, parameter)
        df["station_id"] = station_id
        return df

    def get_latest_months(self, parameter: Parameter, station_id: int) -> pd.DataFrame:
        """Get data from latest months for specified parameter and station.
        """
        json_data = self._query(
            service=f"parameter/{parameter.value}/station/{station_id}/period/latest-months/data.json"
        )
        df = json_to_dataframe(json_data["value"], parameter)
        df.columns = ["date", parameter.name, "quality"]
        df["station_id"] = station_id
        return df

    def get_latest_hour(
        self, parameter: Union[Parameter, List[Parameter]], station_id: int
    ) -> pd.DataFrame:
        """Get data from latest hour for specified parameter and station.
        """
        json_data = self._query(
            service=f"parameter/{parameter.value}/station/{station_id}/period/latest-hour/data.json"
        )
        df = json_to_dataframe(json_data["value"], parameter)
        df.columns = ["date", parameter.name, "quality"]
        df["station_id"] = station_id
        return df

    def get_latest_months_multiple_params(
        self, parameters: List[Parameter], station_id: int
    ):

        for i, parameter in enumerate(parameters):
            json_data = self._query(
                service=f"parameter/{parameter.value}/station/{station_id}/period/latest-months/data.json"
            )
            df_single_param = json_to_dataframe(json_data["value"], parameter)
            df_single_param["station_id"] = station_id
            df_single_param.columns = [
                "date",
                parameter.name,
                "quality_{}".format(parameter.name),
                "station_id",
            ]
            df_single_param.index = df_single_param.date

            if i == 0:
                df = df_single_param.copy()
            else:
                df = pd.concat(
                    [
                        df,
                        df_single_param[
                            [parameter.name, "quality_{}".format(parameter.name)]
                        ],
                    ],
                    join="inner",
                    axis=1,
                )
        return df

    def get_corrected_data_multiple_params(
        self, parameters: List[Parameter], station_id: int
    ):

        for i, parameter in enumerate(parameters):
            df_single_param = self._query_csv(
                service=f"parameter/{parameter.value}/station/{station_id}/period/corrected-archive/data.csv"
            )
            df_single_param = format_archived_dataframe(df_single_param, parameter)
            df_single_param["station_id"] = station_id
            df_single_param.columns = [
                "date",
                parameter.name,
                "quality_{}".format(parameter.name),
                "station_id",
            ]
            df_single_param.index = df_single_param.date

            if i == 0:
                df = df_single_param.copy()
            else:
                df = pd.concat(
                    [
                        df,
                        df_single_param[
                            [parameter.name, "quality_{}".format(parameter.name)]
                        ],
                    ],
                    join="inner",
                    axis=1,
                )
        return df
