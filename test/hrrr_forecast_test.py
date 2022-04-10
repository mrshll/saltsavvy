import datetime
import unittest

from src.hrrr_forecast import HRRRForecast, HRRRVariable


class TestHRRRForecast(unittest.TestCase):
    def setUp(self):
        self.forecast = HRRRForecast(run_hour=datetime.datetime(
            2021, 1, 1, 7), level='sfc', model='fcst')

    def test_get_explorer_url(self):
        self.assertEqual(self.forecast.get_explorer_url(
        ), 'https://hrrrzarr.s3.amazonaws.com/index.html#sfc/20210101/20210101_07z_fcst.zarr/')

    def test_get_https_chunk_url(self):
        tmp_var = HRRRVariable(level='surface', name='TMP')
        self.assertEqual(self.forecast.get_https_chunk_url(tmp_var, "4.3"
                                                           ), 'https://hrrrzarr.s3.amazonaws.com/sfc/20210101/20210101_07z_fcst.zarr/surface/TMP/surface/TMP/0.4.3')

    def test_get_s3_chunk_url(self):
        tmp_var = HRRRVariable(level='surface', name='TMP')
        self.assertEqual(self.forecast.get_s3_chunk_url(tmp_var, "4.3"
                                                        ), 'sfc/20210101/20210101_07z_fcst.zarr/surface/TMP/surface/TMP/0.4.3')


if __name__ == '__main__':
    unittest.main()
