from unittest import TestCase
from unittest.mock import Mock

from data_indexer.cdf_parser.variable_selector.multi_dimension_variable_selector import MultiDimensionVariableSelector
from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector


class TestMultiDimensionVariableSelector(TestCase):
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
    def test_is_subclass_of_variable_selector(self):
        self.assertTrue(issubclass(MultiDimensionVariableSelector, VariableSelector))

    def test_accepts_one_dimensional_timeseries(self):
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

        self.assertTrue(MultiDimensionVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_accepts_higher_dimensional_timeseries(self):
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
        accepted_variable.shape = (1,2)

        self.assertTrue(MultiDimensionVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_does_not_accept_three_dimensional_timeseries(self):
        variable = Mock()
        variable.attrs = {
            "CATDESC": "variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'time_series'
        }
        variable.shape = (1, 2, 1)

        self.assertFalse(MultiDimensionVariableSelector.should_include(variable, self.mock_cdf))


    def test_accepts_two_dimensional_spectrogram(self):
        accepted_variable = Mock()
        accepted_variable.attrs = {
            "CATDESC": "accepted_variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        accepted_variable.shape = (999,23)

        self.assertTrue(MultiDimensionVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_accepts_higher_dimensional_spectrogram(self):
        accepted_variable = Mock()
        accepted_variable.attrs = {
            "CATDESC": "accepted_variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        accepted_variable.shape = (8,999,23)

        self.assertTrue(MultiDimensionVariableSelector.should_include(accepted_variable, self.mock_cdf))

    def test_does_not_accept_four_dimensional_spectrogram(self):
        variable = Mock()
        variable.attrs = {
            "CATDESC": "variable",
            "VAR_TYPE": "data",
            "FIELDNAM": "something",
            "DEPEND_0": "time_col_good",
            "SCALETYP": "linear",
            "SCALEMIN": 1,
            "DISPLAY_TYPE": 'spectrogram'
        }
        variable.shape = (5,6,7,8)

        self.assertFalse(MultiDimensionVariableSelector.should_include(variable, self.mock_cdf))

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

        self.assertFalse(MultiDimensionVariableSelector.should_include(var_with_invalid_log_scalemin, self.mock_cdf))

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

        self.assertFalse(MultiDimensionVariableSelector.should_include(var_with_unknown_time_unit, self.mock_cdf))