from spacepy import pycdf

from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector


class DefaultVariableSelector(VariableSelector):
    acceptable_dimensions = {
        'image': (4,),
        'spectrogram': (2, 3, 4),
        'time_series': (1,)
    }

    @classmethod
    def should_include(cls, var: pycdf.Var, cdf: pycdf.CDF) -> bool:
        if var.attrs["VAR_TYPE"] != "data":
            return False
        display_type = var.attrs.get('DISPLAY_TYPE')
        has_correct_shape = len(var.shape) in cls.acceptable_dimensions.get(display_type,[])
        time_col = var.attrs.get('DEPEND_0')
        time_is_ns = False
        if time_col is not None:
            time_is_ns = cdf[time_col].attrs['UNITS'] == 'ns'

        if var.type() == pycdf.const.CDF_TIME_TT2000.value:
            return False

        zscale = var.attrs['SCALETYP'] if 'SCALETYP' in var.attrs else 'linear'
        scale_is_valid = zscale == 'linear' or var.attrs['SCALEMIN'] != 0
        return has_correct_shape and time_is_ns and scale_is_valid
