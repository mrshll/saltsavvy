import datetime
import unittest

from src import hrrr_forecast, hrrr_source


class TestHRRRForecast(unittest.TestCase):

    def setUp(self):
        self.issue_time = datetime.datetime(2021, 1, 1, 7)
        self.source = hrrr_source.HRRRSource()
        self.test_var = hrrr_source.HRRRVariable(level='surface', name='TMP')

    def test_get_s3_chunk_url(self):
        self.assertEqual(
            self.source.get_s3_chunk_url(self.test_var, self.issue_time,
                                         hrrr_forecast.MODEL_FORECAST, "4.3"),
            'hrrrzarr/sfc/20210101/20210101_07z_fcst.zarr/surface/TMP/surface/TMP/0.4.3'
        )

    def test_get_chunk_id_for_lat_lng(self):
        chunk_id = self.source.get_chunk_id_for_lat_lng(40.7608, -111.8910)
        self.assertEqual(chunk_id, '4.3')

    def test_get_chunk_xy_for_lat_lng(self):
        chunk_xy = self.source.get_chunk_xy_for_lat_lng(40.7608, -111.8910)
        # TODO: assert something!
        self.assertTrue(True)

    def test_fetch_forecast_for_lat_lng(self):
        data = hrrr_forecast.fetch_forecast_for_lat_lng(
            self.test_var, self.issue_time, 40.7608, -111.8910)
        self.assertEqual(len(data), 18)

    def test_fetch_analysis_for_lat_lng(self):
        start_date = datetime.datetime(2022, 1, 1)
        end_date = datetime.datetime(2022, 1, 2)

        data = hrrr_forecast.fetch_analysis_for_lat_lng(
            self.test_var, start_date, end_date, 40.7608, -111.8910)
        self.assertEqual(len(data), 24)


if __name__ == '__main__':
    unittest.main()
