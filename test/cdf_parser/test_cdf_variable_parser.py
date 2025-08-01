import unittest
from pathlib import Path
from unittest.mock import Mock

from spacepy import pycdf

import test
from data_indexer.cdf_parser.cdf_variable_parser import CdfVariableParser, CdfVariableInfo
from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector
from data_indexer.cdf_parser.variable_selector.multi_dimension_variable_selector import MultiDimensionVariableSelector
from data_indexer.cdf_parser.variable_selector.omni_variable_selector import OmniVariableSelector


class TestCdfVariableParser(unittest.TestCase):
    def test_parse_info_from_cdf_returns_expected_info(self):
        expected_info = [
            CdfVariableInfo('Roll_Angle', 'Angle between nominal ram and actual ram, 0 in encounter', 'time_series',
                            'degrees', 'Roll Angle'),
            CdfVariableInfo('Sun_Angle', 'Angle between TPS and Sun, 0 in encounter', 'time_series', 'degrees', "Sun Angle"),
            CdfVariableInfo('Clock_Angle', 'angle of off-pointing from ecliptic north when not in encounter',
                            'time_series', 'degrees', "Clock Angle"),
            CdfVariableInfo('HCI_Lat', 'HCI latitude', 'time_series', 'degrees', "HCI latitude"),
            CdfVariableInfo('HCI_Lon', 'HCI longitude', 'time_series', 'degrees', "HCI longitude"),
            CdfVariableInfo('HCI_R', 'Heliocentric distance', 'time_series', 'AU', "R"),
            CdfVariableInfo('HGC_R', 'Heliocentric distance', 'time_series', 'AU', "R"),
            CdfVariableInfo('Spiral_HETA', 'HETA look angle with nominal parker spiral', 'time_series', 'degrees', "HETA Angle"),
            CdfVariableInfo('HGC_Lat', 'HGC latitude', 'time_series', 'degrees', "HGC latitude"),
            CdfVariableInfo('HGC_Lon', 'HGC longitude', 'time_series', 'degrees', "HGC longitude"),
            CdfVariableInfo('Spiral_LET1A', 'LET1A look angle with nominal parker spiral', 'time_series', 'degrees', "LET1A Angle"),
            CdfVariableInfo('Spiral_LET2C', 'LET2C look angle with nominal parker spiral', 'time_series', 'degrees', "LET2C Angle"),
            CdfVariableInfo('Spiral_Lo', 'Lo look angle with nominal parker spiral', 'time_series', 'degrees', "Lo Angle"),
            CdfVariableInfo('Ram_Pointing', 'Spacecraft is ram pointing', 'time_series', '', "Ram Pointing"),
            CdfVariableInfo('Umbra_Pointing', 'Spacecraft is umbra pointing', 'time_series', '', "Umbra Pointing"),
        ]

        cdf_path = str(Path(test.__file__).parent / 'test_data/test.cdf')
        with pycdf.CDF(cdf_path) as cdf:
            parsed_info = CdfVariableParser.parse_info_from_cdf(cdf, DefaultVariableSelector)

        self.assertEqual(expected_info, parsed_info)

    def test_parse_info_from_omni_data(self):
        cdf_path = str(Path(test.__file__).parent / 'test_data/omni2_h0_mrg1hr_20240101_v01.cdf')
        with pycdf.CDF(cdf_path) as cdf:
            parsed_info = CdfVariableParser.parse_info_from_cdf(cdf, OmniVariableSelector)

        variable_names = [variable.variable_name for variable in parsed_info]
        self.assertNotIn('Epoch', variable_names)
        self.assertEqual(48, len(variable_names))

    def test_parse_info_from_epilo_cdf_filters_out_variables_that_are_missing_key_features(self):
        mock_cdf = Mock()

        mock_cdf.attrs = {'Data_version': "99", 'Logical_source': "lsource",
                          "Descriptor": "ISOIS-EPILO>Integrated Science Investigation of the Sun, Energetic Particle Instrument Lo"}
        expected_info = [
            CdfVariableInfo("var0", 'var_not_filtered', 'spectrogram', 'degrees',""),
            CdfVariableInfo("var7", "var_not_filtered_for_nonzero_min_and_log", 'spectrogram', 'degrees',""),
            CdfVariableInfo("var9", "var_not_filtered_for_scale", 'spectrogram', 'degrees',""),
            CdfVariableInfo("var8", "var_not_filtered_for_scaletype_missing_and_scalemin_zero", "spectrogram",
                            'degrees',""),
            CdfVariableInfo("var13", "var_not_filtered_for_timeseries_shape_with_look_direction", 'time_series',
                            'degrees',""),
            CdfVariableInfo("var5", "var_not_filtered_linear", 'spectrogram', 'degrees',""),
            CdfVariableInfo("var12", "var_that_is_not_filtered_three_dimensional_spectrogram", 'spectrogram',
                            'degrees',""),
        ]

        var_that_is_not_filtered = Mock()
        var_that_is_not_filtered.attrs = {
            "CATDESC": "var_not_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }
        var_that_is_not_filtered.shape = (1, 2)

        var_that_is_not_filtered_three_dimensional_spectrogram = Mock()
        var_that_is_not_filtered_three_dimensional_spectrogram.shape = (1, 2, 3)
        var_that_is_not_filtered_three_dimensional_spectrogram.attrs = {
            "CATDESC": "var_that_is_not_filtered_three_dimensional_spectrogram",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "DEPEND_1": "energy_col",
            "DEPEND_2": "look_direction_col",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }

        var_filtered_for_incorrect_shape = Mock()
        var_filtered_for_incorrect_shape.attrs = {
            "CATDESC": "var_filtered_for_incorrect_shape",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }
        var_filtered_for_incorrect_shape.shape = (1, 2, 3, 4)

        var_not_filtered_for_linear_scaletype_and_scalemin_nonzero = Mock()
        var_not_filtered_for_linear_scaletype_and_scalemin_nonzero.attrs = {
            "CATDESC": "var_not_filtered_linear",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
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
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
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
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }
        var_not_filtered_for_scaletype_log_and_scalemin_nonzero.shape = (1, 2)

        var_not_filtered_for_scaletype_missing_and_scalemin_zero = Mock()
        var_not_filtered_for_scaletype_missing_and_scalemin_zero.attrs = {
            "CATDESC": "var_not_filtered_for_scaletype_missing_and_scalemin_zero",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALEMIN": 0,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }
        var_not_filtered_for_scaletype_missing_and_scalemin_zero.shape = (1, 2)

        var_not_filtered_for_scaletype_missing_and_scalemin_nonzero = Mock()
        var_not_filtered_for_scaletype_missing_and_scalemin_nonzero.attrs = {
            "CATDESC": "var_not_filtered_for_scale",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
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
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }
        var_filtered_for_wrong_time_units.shape = (1, 2)

        var_filtered_for_wrong_timeseries_shape = Mock()
        var_filtered_for_wrong_timeseries_shape.attrs = {
            "CATDESC": "var_filtered_for_wrong_timeseries_shape",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series',
            "UNITS": 'degrees'
        }
        var_filtered_for_wrong_timeseries_shape.shape = (1, 2, 3)

        var_not_filtered_for_timeseries_shape_with_look_direction = Mock()
        var_not_filtered_for_timeseries_shape_with_look_direction.attrs = {
            "CATDESC": "var_not_filtered_for_timeseries_shape_with_look_direction",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series',
            "UNITS": 'degrees'
        }
        var_not_filtered_for_timeseries_shape_with_look_direction.shape = (1, 2)

        mock_cdf.items.return_value = {
            'var0': var_that_is_not_filtered,
            'var1': var_filtered_for_incorrect_shape,
            'var5': var_not_filtered_for_linear_scaletype_and_scalemin_nonzero,
            'var6': var_filtered_for_scaletype_log_and_scalemin_zero,
            'var7': var_not_filtered_for_scaletype_log_and_scalemin_nonzero,
            'var8': var_not_filtered_for_scaletype_missing_and_scalemin_zero,
            'var9': var_not_filtered_for_scaletype_missing_and_scalemin_nonzero,
            'var10': var_filtered_for_wrong_time_units,
            'var11': var_filtered_for_wrong_timeseries_shape,
            'var12': var_that_is_not_filtered_three_dimensional_spectrogram,
            'var13': var_not_filtered_for_timeseries_shape_with_look_direction
        }.items()

        mock_time_column_good = Mock()
        mock_time_column_good.attrs = {'DELTA_MINUS_VAR': '', 'DELTA_PLUS_VAR': '', 'UNITS': 'ns'}
        mock_time_col_unit_ms = Mock()
        mock_time_col_unit_ms.attrs = {'DELTA_MINUS_VAR': '', 'DELTA_PLUS_VAR': '', 'UNITS': 'ms'}
        cdf_items = {'time_col_good': mock_time_column_good,
                     'time_col_unit_ms': mock_time_col_unit_ms}
        mock_cdf.__getitem__ = Mock()
        mock_cdf.__getitem__.side_effect = lambda key: cdf_items[key]

        returned_info = CdfVariableParser.parse_info_from_cdf(mock_cdf, MultiDimensionVariableSelector)

        self.assertEqual(expected_info, returned_info)

    def test_parse_info_from_non_epilo_cdf_filters_out_variables_that_are_missing_key_features(self):
        mock_cdf = Mock()

        mock_cdf.attrs = {'Data_version': "99", 'Logical_source': "lsource",
                          "Descriptor": "ISOIS-EPIHI>Integrated Science Investigation of the Sun, Energetic Particle Instrument Hi"}
        expected_info = [
            CdfVariableInfo("var0", 'var_not_filtered', 'spectrogram', 'degrees',""),
            CdfVariableInfo("var1", 'var_that_not_filtered_three_dimensional_spectrogram', 'spectrogram', 'degrees',""),
        ]

        var_that_is_not_filtered = Mock()
        var_that_is_not_filtered.attrs = {
            "CATDESC": "var_not_filtered",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }
        var_that_is_not_filtered.shape = (1, 2)

        var_that_is_not_filtered_three_dimensional_spectrogram = Mock()
        var_that_is_not_filtered_three_dimensional_spectrogram.shape = (1, 2, 3)
        var_that_is_not_filtered_three_dimensional_spectrogram.attrs = {
            "CATDESC": "var_that_not_filtered_three_dimensional_spectrogram",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "DEPEND_1": "energy_col",
            "DEPEND_2": "look_direction_col",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram',
            "UNITS": 'degrees'
        }

        var_filtered_for_timeseries_shape_with_too_many_dimensions = Mock()
        var_filtered_for_timeseries_shape_with_too_many_dimensions.attrs = {
            "CATDESC": "var_filtered_for_timeseries_shape_with_too_many_dimensions",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series',
            "UNITS": 'degrees'
        }
        var_filtered_for_timeseries_shape_with_too_many_dimensions.shape = (1, 2)

        mock_cdf.items.return_value = {
            'var0': var_that_is_not_filtered,
            'var1': var_that_is_not_filtered_three_dimensional_spectrogram,
            'var2': var_filtered_for_timeseries_shape_with_too_many_dimensions,
        }.items()

        mock_time_column_good = Mock()
        mock_time_column_good.attrs = {'DELTA_MINUS_VAR': '', 'DELTA_PLUS_VAR': '', 'UNITS': 'ns'}
        mock_time_col_unit_ms = Mock()
        mock_time_col_unit_ms.attrs = {'DELTA_MINUS_VAR': '', 'DELTA_PLUS_VAR': '', 'UNITS': 'ms'}
        cdf_items = {'time_col_good': mock_time_column_good, 'time_col_unit_ms': mock_time_col_unit_ms}
        mock_cdf.__getitem__ = Mock()
        mock_cdf.__getitem__.side_effect = lambda key: cdf_items[key]

        returned_info = CdfVariableParser.parse_info_from_cdf(mock_cdf, DefaultVariableSelector)

        self.assertEqual(expected_info, returned_info)
