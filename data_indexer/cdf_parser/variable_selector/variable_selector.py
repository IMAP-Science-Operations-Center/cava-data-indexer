from abc import ABC, abstractmethod

from spacepy import pycdf


class VariableSelector(ABC):
    @classmethod
    @abstractmethod
    def should_include(cls, var: pycdf.Var, cdf: pycdf.CDF) -> bool:
        raise NotImplementedError
