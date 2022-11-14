import unittest

from src.cdf_variable_parser import CdfVariableParser


class TestCdfVariableParser(unittest.TestCase):
    def test_parse_variables_from_cdf_returns_list_of_descriptions(self):
        expected_descriptions = {
            'Angle between TPS and Sun, 0 in encounter v13': 'Sun_Angle',
            'Angle between nominal ram and actual ram, 0 in encounter v13': 'Roll_Angle',
            'HCI latitude v13': 'HCI_Lat',
            'HCI longitude v13': 'HCI_Lon',
            'HETA look angle with nominal parker spiral v13': 'Spiral_HETA',
            'HGC latitude v13': 'HGC_Lat',
            'HGC longitude v13': 'HGC_Lon',
            'Heliocentric distance v13': 'HGC_R',
            'LET1A look angle with nominal parker spiral v13': 'Spiral_LET1A',
            'LET2C look angle with nominal parker spiral v13': 'Spiral_LET2C',
            'Lo look angle with nominal parker spiral v13': 'Spiral_Lo',
            'Spacecraft is ram pointing v13': 'Ram_Pointing',
            'Spacecraft is umbra pointing v13': 'Umbra_Pointing',
            'angle of off-pointing from ecliptic north when not in encounter v13': 'Clock_Angle'
        }

        cdf_path = './test_data/test.cdf'
        descriptions = CdfVariableParser.parse_variables_from_cdf(cdf_path)

        self.assertEqual(expected_descriptions, descriptions)

    def test_parse_variables_from_cdf_bytes_returns_list_of_descriptions(self):
        expected_descriptions = {
            'Angle between TPS and Sun, 0 in encounter v13': 'Sun_Angle',
            'Angle between nominal ram and actual ram, 0 in encounter v13': 'Roll_Angle',
            'HCI latitude v13': 'HCI_Lat',
            'HCI longitude v13': 'HCI_Lon',
            'HETA look angle with nominal parker spiral v13': 'Spiral_HETA',
            'HGC latitude v13': 'HGC_Lat',
            'HGC longitude v13': 'HGC_Lon',
            'Heliocentric distance v13': 'HGC_R',
            'LET1A look angle with nominal parker spiral v13': 'Spiral_LET1A',
            'LET2C look angle with nominal parker spiral v13': 'Spiral_LET2C',
            'Lo look angle with nominal parker spiral v13': 'Spiral_Lo',
            'Spacecraft is ram pointing v13': 'Ram_Pointing',
            'Spacecraft is umbra pointing v13': 'Umbra_Pointing',
            'angle of off-pointing from ecliptic north when not in encounter v13': 'Clock_Angle'
        }
        cdf_path = './test_data/test.cdf'
        with open(cdf_path, 'rb') as file:
            cdf_bytes = file.read()

        descriptions = CdfVariableParser.parse_variables_from_cdf_bytes(cdf_bytes)

        self.assertEqual(expected_descriptions, descriptions)

