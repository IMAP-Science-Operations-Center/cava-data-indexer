from spacepy import pycdf

from data_indexer.cdf_parser.variable_selector.variable_selector import VariableSelector


class OmniVariableSelector(VariableSelector):
    acceptable_dimensions = {
        'time_series': (1,),
    }

    @classmethod
    def should_include(cls, var: pycdf.Var, cdf: pycdf.CDF) -> bool:
        display_type = var.attrs.get('DISPLAY_TYPE')
        has_correct_shape = len(var.shape) in cls.acceptable_dimensions.get(display_type,[])
        zscale = var.attrs['SCALETYP'] if 'SCALETYP' in var.attrs else 'linear'
        scale_is_valid = zscale == 'linear' or var.attrs['SCALEMIN'] != 0
        return has_correct_shape and scale_is_valid


