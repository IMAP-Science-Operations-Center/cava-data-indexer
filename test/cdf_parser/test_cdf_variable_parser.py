import unittest
from pathlib import Path
from unittest.mock import Mock

from spacepy import pycdf

import test
from src.cdf_parser.cdf_variable_parser import CdfVariableParser, CdfVariableInfo


class TestCdfVariableParser(unittest.TestCase):
    def test_parse_variables_from_cdf_returns_expected_descriptions(self):
        expected_descriptions = [
            CdfVariableInfo('Roll_Angle', 'Angle between nominal ram and actual ram, 0 in encounter', 'time_series'),
            CdfVariableInfo('Sun_Angle', 'Angle between TPS and Sun, 0 in encounter', 'time_series'),
            CdfVariableInfo('Clock_Angle', 'angle of off-pointing from ecliptic north when not in encounter',
                            'time_series'),
            CdfVariableInfo('HCI_Lat', 'HCI latitude', 'time_series'),
            CdfVariableInfo('HCI_Lon', 'HCI longitude', 'time_series'),
            CdfVariableInfo('HCI_R', 'Heliocentric distance', 'time_series'),
            CdfVariableInfo('HGC_R', 'Heliocentric distance', 'time_series'),
            CdfVariableInfo('Spiral_HETA', 'HETA look angle with nominal parker spiral', 'time_series'),
            CdfVariableInfo('HGC_Lat', 'HGC latitude', 'time_series'),
            CdfVariableInfo('HGC_Lon', 'HGC longitude', 'time_series'),
            CdfVariableInfo('Spiral_LET1A', 'LET1A look angle with nominal parker spiral', 'time_series'),
            CdfVariableInfo('Spiral_LET2C', 'LET2C look angle with nominal parker spiral', 'time_series'),
            CdfVariableInfo('Spiral_Lo', 'Lo look angle with nominal parker spiral', 'time_series'),
            CdfVariableInfo('Ram_Pointing', 'Spacecraft is ram pointing', 'time_series'),
            CdfVariableInfo('Umbra_Pointing', 'Spacecraft is umbra pointing', 'time_series'),
        ]

        cdf_path = str(Path(test.__file__).parent / 'test_data/test.cdf')
        with pycdf.CDF(cdf_path) as cdf:
            descriptions = CdfVariableParser.parse_info_from_cdf(cdf)

        self.assertEqual(expected_descriptions, descriptions)

    def test_parse_variables_from_cdf_bytes_filter_out_variables_that_are_missing_key_features(self):
        mock_cdf = Mock()

        mock_cdf.attrs = {'Data_version': "99", 'Logical_source': "lsource"}

        expected_descriptions = [
            CdfVariableInfo("var0",'var_not_filtered','spectrogram'),
            CdfVariableInfo("var7","var_not_filtered_for_nonzero_min_and_log", 'spectrogram'),
            CdfVariableInfo("var9","var_not_filtered_for_scale", 'spectrogram'),
            CdfVariableInfo("var5","var_not_filtered_linear",'spectrogram'),
            ]

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

        mock_cdf.items.return_value = {
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
        mock_cdf.__getitem__ = Mock()
        mock_cdf.__getitem__.side_effect = lambda key: cdf_items[key]

        returned_info = CdfVariableParser.parse_info_from_cdf(mock_cdf)

        self.assertEqual(expected_descriptions, returned_info)
