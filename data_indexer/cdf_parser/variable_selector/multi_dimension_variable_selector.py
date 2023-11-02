from data_indexer.cdf_parser.variable_selector.default_variable_selector import DefaultVariableSelector


class MultiDimensionVariableSelector(DefaultVariableSelector):
    acceptable_dimensions = {
        'spectrogram': (2, 3),
        'time_series': (1, 2)
    }
