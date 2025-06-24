import unittest
from unittest.mock import Mock

from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector


class TestDefaultVariableSelector(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_cdf = Mock()
        self.mock_cdf.attrs = {'Data_version': "99", 'Logical_source': "lsource",
                               "Descriptor": "ISOIS-EPILO>Integrated Science Investigation of the Sun, Energetic Particle Instrument Lo"}
        mock_time_column_good = Mock()
        mock_time_column_good.attrs = {'UNITS': 'ns'}
        mock_time_column_unknown = Mock()
        mock_time_column_unknown.attrs = {'UNITS': 'unknown'}
        cdf_items = {"time_col_good": mock_time_column_good, "time_col_unknown": mock_time_column_unknown}
        self.mock_cdf.__getitem__ = Mock()
        self.mock_cdf.__getitem__.side_effect = lambda key: cdf_items[key]

    def test_accepts_expected_timeseries_variable(self):
        accepted_variable = Mock()
        accepted_variable.attrs = {
            "CATDESC": "accepted_variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series'
        }
        accepted_variable.shape = (1,)

        self.assertTrue(DefaultVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_accepts_expected_2_dimensional_spectrogram_variable(self):
        accepted_variable = Mock()
        accepted_variable.attrs = {
            "CATDESC": "accepted_variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "log",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        accepted_variable.shape = (1, 2)

        self.assertTrue(DefaultVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_accepts_expected_3_dimensional_spectrogram_variable(self):
        accepted_variable = Mock()
        accepted_variable.attrs = {
            "CATDESC": "accepted_variable_with_3",
            "VAR_TYPE": "data",
            "FIELDNAM": "now in 3D",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "log",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        accepted_variable.shape = (1, 2, 3)

        self.assertTrue(DefaultVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_accepts_expected_4_dimensional_spectrogram_variable(self):
        accepted_variable = Mock()
        accepted_variable.attrs = {
            "CATDESC": "a 4 dimensional spectrogram",
            "VAR_TYPE": "data",
            "FIELDNAM": "now in 4D!!!!",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "log",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        accepted_variable.shape = (1, 2, 3, 4)

        self.assertTrue(DefaultVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_does_not_accept_variable_with_bad_shape_for_timeseries(self):
        bad_shape_variable_timeseries = Mock()
        bad_shape_variable_timeseries.attrs = {
            "CATDESC": "bad_shape_variable_timeseries",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series'
        }
        bad_shape_variable_timeseries.shape = (1, 2)

        self.assertFalse(DefaultVariableSelector.should_include(bad_shape_variable_timeseries, self.mock_cdf))

    def test_does_not_accept_variable_with_bad_shape_for_spectrogram(self):
        bad_shape_variable_spectrogram = Mock()
        bad_shape_variable_spectrogram.attrs = {
            "CATDESC": "bad_shape_variable_spectrogram",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        bad_shape_variable_spectrogram.shape = (9,)

        self.assertFalse(DefaultVariableSelector.should_include(bad_shape_variable_spectrogram, self.mock_cdf))

    def test_does_not_accept_variable_with_no_plot_display_type(self):
        no_plot_variable = Mock()
        no_plot_variable.attrs = {
            "CATDESC": "no_plot_variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'no_plot'
        }
        no_plot_variable.shape = (1,2)

        self.assertFalse(DefaultVariableSelector.should_include(no_plot_variable, self.mock_cdf))

    def test_does_not_accept_log_scale_axis_with_invalid_scalemin(self):
        var_with_invalid_log_scalemin = Mock()
        var_with_invalid_log_scalemin.attrs = {
            "CATDESC": "var_with_unknown_time_unit",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "log",
            "SCALEMIN": 0,
            "DISPLAY_TYPE": 'time_series'
        }
        var_with_invalid_log_scalemin.shape = (1,)

        self.assertFalse(DefaultVariableSelector.should_include(var_with_invalid_log_scalemin, self.mock_cdf))

    def test_does_not_accept_variable_with_unknown_time_unit(self):
        var_with_unknown_time_unit = Mock()
        var_with_unknown_time_unit.attrs = {
            "CATDESC": "var_with_unknown_time_unit",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_unknown",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series'
        }
        var_with_unknown_time_unit.shape = (1,)

        self.assertFalse(DefaultVariableSelector.should_include(var_with_unknown_time_unit, self.mock_cdf))

    def test_does_accepts_map_variable(self):
        map_variable = Mock()
        map_variable.attrs = {
            "CATDESC": "Map of Intensity of ENAs",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'image'
        }
        map_variable.shape = (1, 2, 3, 4)

        self.assertTrue(DefaultVariableSelector.should_include(map_variable, self.mock_cdf))

    def test_does_accepts_variable_with_no_time_var(self):
        missing_depend_0_variable = Mock()
        missing_depend_0_variable.attrs = {
            "CATDESC": "Does not depend on time",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'image'
        }
        missing_depend_0_variable.shape = (1, 2, 3, 4)

        self.assertFalse(DefaultVariableSelector.should_include(missing_depend_0_variable, self.mock_cdf))

if __name__ == '__main__':
    unittest.main()
