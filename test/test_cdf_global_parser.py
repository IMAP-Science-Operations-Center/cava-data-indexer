import unittest

from src.cdf_global_parser import CdfGlobalParser


class TestCdfGlobalParser(unittest.TestCase):
    def test_parse_global_variables_from_cdf(self):
        expected_global_variables = {"Logical_source":"psp_isois_l2-ephem"}

        parser = CdfGlobalParser()
        cdf_path = './test_data/test.cdf'
        global_variables = parser.parse_global_variables_from_cdf(cdf_path)

        self.assertEqual(expected_global_variables, global_variables)


