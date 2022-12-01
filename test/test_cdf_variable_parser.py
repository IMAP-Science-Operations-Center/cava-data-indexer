import unittest
from unittest.mock import patch, Mock

from src.cdf_variable_parser import CdfVariableParser, CdfFileInfo


class TestCdfVariableParser(unittest.TestCase):
    def test_parse_variables_from_cdf_returns_cdf_file_info(self):
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

        expected_cdf_file_info = CdfFileInfo(
            logical_source="psp_isois_l2-ephem",
            data_version="13",
            variable_desc_to_key_dict=expected_descriptions)

        cdf_path = './test_data/test.cdf'
        descriptions = CdfVariableParser.parse_info_from_cdf(cdf_path)

        self.assertEqual(expected_cdf_file_info, descriptions)

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


        expected_cdf_file_info = CdfFileInfo(
            logical_source="psp_isois_l2-ephem",
            data_version="13",
            variable_desc_to_key_dict=expected_descriptions)

        descriptions = CdfVariableParser.parse_info_from_cdf_bytes(cdf_bytes)

        self.assertEqual(expected_cdf_file_info, descriptions)

    @patch('src.cdf_variable_parser.pycdf.CDF')
    def test_parse_variables_from_cdf_bytes_filter_out_variables_that_are_missing_key_features(self, mock_CDF_class):
        mock_cdf_instance = Mock()
        mock_CDF_class.return_value = mock_cdf_instance

        mock_cdf_instance.attrs = {'Data_version': "99", 'Logical_source': "lsource"}

        expected_descriptions = {'var_not_filtered v99': "var0", "var_not_filtered_linear v99": "var5",
                                 "var_not_filtered_for_nonzero_min_and_log v99": "var7",
                                 "var_not_filtered_for_scale v99": "var9"}

        var_that_is_not_filtered = Mock()
        var_that_is_not_filtered.attrs = {
            "CATDESC": "var_not_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_that_is_not_filtered.shape = (1, 2)

        var_filtered_for_incorrect_shape = Mock()
        var_filtered_for_incorrect_shape.attrs = {
            "CATDESC": "var_filtered_for_incorrect_shape",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_incorrect_shape.shape = (1, 2, 3)

        var_filtered_for_missing_fieldnam = Mock()
        var_filtered_for_missing_fieldnam.attrs = {
            "CATDESC": "var_not_filtered",
            "VAR_TYPE": "data",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_missing_fieldnam.shape = (1, 2)

        var_filtered_for_missing_delta_minus = Mock()
        var_filtered_for_missing_delta_minus.attrs = {
            "CATDESC": "var_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_no_delta_minus",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_missing_delta_minus.shape = (1, 2)

        var_filtered_for_missing_delta_plus = Mock()
        var_filtered_for_missing_delta_plus.attrs = {
            "CATDESC": "var_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_no_delta_plus",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_missing_delta_plus.shape = (1, 2)

        var_not_filtered_for_linear_scaletype_and_scalemin_nonzero = Mock()
        var_not_filtered_for_linear_scaletype_and_scalemin_nonzero.attrs = {
            "CATDESC": "var_not_filtered_linear",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_not_filtered_for_linear_scaletype_and_scalemin_nonzero.shape = (1, 2)

        var_filtered_for_scaletype_log_and_scalemin_zero = Mock()
        var_filtered_for_scaletype_log_and_scalemin_zero.attrs = {
            "CATDESC": "var_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "log",
            "SCALEMIN": 0,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_scaletype_log_and_scalemin_zero.shape = (1, 2)

        var_not_filtered_for_scaletype_log_and_scalemin_nonzero = Mock()
        var_not_filtered_for_scaletype_log_and_scalemin_nonzero.attrs = {
            "CATDESC": "var_not_filtered_for_nonzero_min_and_log",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "log",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_not_filtered_for_scaletype_log_and_scalemin_nonzero.shape = (1, 2)

        var_filtered_for_scaletype_missing_and_scalemin_zero = Mock()
        var_filtered_for_scaletype_missing_and_scalemin_zero.attrs = {
            "CATDESC": "var_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALEMIN": 0,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_scaletype_missing_and_scalemin_zero.shape = (1, 2)

        var_not_filtered_for_scaletype_missing_and_scalemin_nonzero = Mock()
        var_not_filtered_for_scaletype_missing_and_scalemin_nonzero.attrs = {
            "CATDESC": "var_not_filtered_for_scale",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_not_filtered_for_scaletype_missing_and_scalemin_nonzero.shape = (1, 2)

        var_filtered_for_wrong_time_units = Mock()
        var_filtered_for_wrong_time_units.attrs = {
            "CATDESC": "var_filtered_for_wrong_time",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_unit_ms",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        var_filtered_for_wrong_time_units.shape = (1, 2)

        var_filtered_for_wrong_timeseries_shape = Mock()
        var_filtered_for_wrong_timeseries_shape.attrs = {
            "CATDESC": "var_filtered_for_timeseries_shape",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series'
        }
        var_filtered_for_wrong_timeseries_shape.shape = (1, 2)

        mock_cdf_instance.items.return_value = {
            'var0': var_that_is_not_filtered,
            'var1': var_filtered_for_incorrect_shape,
            'var2': var_filtered_for_missing_fieldnam,
            'var3': var_filtered_for_missing_delta_minus,
            'var4': var_filtered_for_missing_delta_plus,
            'var5': var_not_filtered_for_linear_scaletype_and_scalemin_nonzero,
            'var6': var_filtered_for_scaletype_log_and_scalemin_zero,
            'var7': var_not_filtered_for_scaletype_log_and_scalemin_nonzero,
            'var8': var_filtered_for_scaletype_missing_and_scalemin_zero,
            'var9': var_not_filtered_for_scaletype_missing_and_scalemin_nonzero,
            'var10': var_filtered_for_wrong_time_units,
            'var11': var_filtered_for_wrong_timeseries_shape
        }.items()

        mock_time_column_good = Mock()
        mock_time_column_good.attrs = {'DELTA_MINUS_VAR': '', 'DELTA_PLUS_VAR': '', 'UNITS': 'ns'}
        mock_time_col_no_delta_minus = Mock()
        mock_time_col_no_delta_minus.attrs = {'DELTA_PLUS_VAR': '', 'UNITS': 'ns'}
        mock_time_col_no_delta_plus = Mock()
        mock_time_col_no_delta_plus.attrs = {'DELTA_MINUS_VAR': '', 'UNITS': 'ns'}
        mock_time_col_unit_ms = Mock()
        mock_time_col_unit_ms.attrs = {'DELTA_MINUS_VAR': '', 'DELTA_PLUS_VAR': '', 'UNITS': 'ms'}
        cdf_items = {'time_col_good': mock_time_column_good, 'time_col_no_delta_minus': mock_time_col_no_delta_minus,
                     'time_col_no_delta_plus': mock_time_col_no_delta_plus, 'time_col_unit_ms': mock_time_col_unit_ms}
        mock_cdf_instance.__getitem__ = Mock()
        mock_cdf_instance.__getitem__.side_effect = lambda key: cdf_items[key]

        returned_info = CdfVariableParser.parse_info_from_cdf("")

        self.assertEqual(expected_descriptions, returned_info.variable_desc_to_key_dict)
