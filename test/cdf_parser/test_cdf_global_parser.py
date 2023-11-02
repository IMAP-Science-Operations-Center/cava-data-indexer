import unittest
from datetime import date
from pathlib import Path

from spacepy import pycdf

import test
from data_indexer.cdf_parser.cdf_global_parser import CdfGlobalParser, CdfGlobalInfo


class TestCdfGlobalParser(unittest.TestCase):
    def test_parse_global_variables_from_isois_cdf(self):
        expected_global_info = CdfGlobalInfo(logical_source="psp_isois_l2-ephem", data_version="13",
                                             logical_source_description="Parker Solar Probe ISOIS Level 2 ephem",
                                             generation_date=date(2022, 7, 27))

        cdf_path = str(Path(test.__file__).parent / 'test_data/test.cdf')

        with pycdf.CDF(cdf_path) as cdf:
            global_info = CdfGlobalParser.parse_global_variables_from_cdf(cdf)

        self.assertEqual(expected_global_info, global_info)


    def test_parse_global_variables_from_fields_cdf(self):
        expected_global_info = CdfGlobalInfo(logical_source="psp_fld_l2_mag_RTN_4_Sa_per_Cyc", data_version="02",
                                             logical_source_description="PSP FIELDS 4 samples per cycle cadence Fluxgate Magnetometer (MAG) data in RTN coordinates",
                                             generation_date=date(2023, 2, 18))

        cdf_path = str(Path(test.__file__).parent / 'test_data/psp_fld_l2_mag_rtn_4_sa_per_cyc_20230101_v02.cdf')

        with pycdf.CDF(cdf_path) as cdf:
            global_info = CdfGlobalParser.parse_global_variables_from_cdf(cdf)

        self.assertEqual(expected_global_info, global_info)

    def test_raises_value_error_if_cdf_generation_date_fails_to_be_parsed(self):
        with self.assertRaises(ValueError) as err:
            cdf_path = str(Path(test.__file__).parent / 'test_data/unsupported_generation_date.cdf')
            with pycdf.CDF(cdf_path) as cdf:
                global_info = CdfGlobalParser.parse_global_variables_from_cdf(cdf)

        self.assertEqual("Failed to parse generation date `not a date` from CDF `unsupported_generation_date.cdf`", str(err.exception))