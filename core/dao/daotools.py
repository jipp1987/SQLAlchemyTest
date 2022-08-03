# FILTER
import enum
from collections import namedtuple

FilterType = namedtuple('FilterType', ['value', 'filter_keyword'])
"""Tupla para propiedades de EnumFilterTypes. La uso para poder añadirle una propiedad al enumerado, aparte del propio
valor."""


class EnumFilterTypes(enum.Enum):
    """Enumerado de tipos de filtros."""

    @property
    def filter_keyword(self):
        return self.value.filter_keyword

    EQUALS = FilterType(1, '=')
    NOT_EQUALS = FilterType(2, '<>')
    LIKE = FilterType(3, 'LIKE')
    NOT_LIKE = FilterType(4, 'NOT LIKE')
    IN = FilterType(5, 'IN')
    NOT_IN = FilterType(6, 'NOT IN')
    LESS_THAN = FilterType(7, '<')
    LESS_THAN_OR_EQUALS = FilterType(8, '<=')
    GREATER_THAN = FilterType(9, '>')
    GREATER_THAN_OR_EQUALS = FilterType(10, '>=')
    BETWEEN = FilterType(11, 'BETWEEN')
    STARTS_WITH = FilterType(12, 'LIKE')
    ENDS_WITH = FilterType(13, 'LIKE')


OperatorType = namedtuple('OperatorType', ['value', 'operator_keyword'])
"""Tupla para propiedades de EnumOperatorTypes. La uso para poder añadirle una propiedad al enumerado, aparte del propio
valor."""


class EnumOperatorTypes(enum.Enum):
    """Enumerado de tipos de operadores para filtros."""

    @property
    def operator_keyword(self):
        return self.value.operator_keyword

    AND = OperatorType(1, 'AND')
    OR = OperatorType(2, 'OR')


AggregateFunction = namedtuple('AggregateFunction', ['value', 'function_keyword'])
"""Tupla para propiedades de EnumAggregateFunctions. La uso para poder añadirle una propiedad al enumerado, 
aparte del propio valor."""


class EnumAggregateFunctions(enum.Enum):
    """Enumerado de funciones de agregado."""

    @property
    def function_keyword(self):
        return self.value.function_keyword

    COUNT = AggregateFunction(1, 'COUNT')
    MIN = AggregateFunction(2, 'MIN')
    MAX = AggregateFunction(3, 'MAX')


class FilterClause(object):
    """Clase para modelado de cláusulas WHERE para MySQL."""

    def __init__(self, field_name: str, filter_type: (EnumFilterTypes, str), object_to_compare: any,
                 operator_type: (EnumOperatorTypes, str) = None, start_parenthesis: int = None,
                 end_parenthesis: int = None):
        self.field_name = field_name
        """Nombre del campo."""
        self.filter_type = filter_type if isinstance(filter_type, EnumFilterTypes) else EnumFilterTypes[filter_type]
        """Tipo de filtro."""
        self.object_to_compare = object_to_compare
        """Objeto a comparar."""
        self.operator_type = (operator_type if isinstance(operator_type, EnumOperatorTypes)
                              else EnumOperatorTypes[operator_type]) if operator_type is not None \
            else EnumOperatorTypes.AND
        """Tipo de operador que conecta con el filtro inmediatamente anterior. Si null, se asume que es AND."""
        self.start_parenthesis = start_parenthesis
        """Número de paréntesis al principio."""
        self.end_parenthesis = end_parenthesis
        """Número de paréntesis al final."""
