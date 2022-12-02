import unittest
from pathlib import Path

from spacepy import pycdf

import test
from src.cdf_parser.cdf_global_parser import CdfGlobalParser, CdfGlobalInfo


class TestCdfGlobalParser(unittest.TestCase):
    def test_parse_global_variables_from_cdf(self):
        expected_global_info = CdfGlobalInfo(logical_source="psp_isois_l2-ephem",data_version="13")

        cdf_path = str(Path(test.__file__).parent / 'test_data/test.cdf')

        with pycdf.CDF(cdf_path) as cdf:
            global_info = CdfGlobalParser.parse_global_variables_from_cdf(cdf)

        self.assertEqual(expected_global_info, global_info)


