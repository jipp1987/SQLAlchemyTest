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
    STARTS_WITH = FilterType(11, 'LIKE')
    ENDS_WITH = FilterType(12, 'LIKE')


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


# ORDER BYs
OrderByType = namedtuple('OrderByType', ['value', 'order_by_keyword'])
"""Tupla para propiedades de EnumOrderByTypes. La uso para poder añadirle una propiedad al enumerado, aparte del propio
valor."""


class EnumOrderByTypes(enum.Enum):
    """Enumerado de tipos de OrderBy."""

    @property
    def order_by_keyword(self):
        return self.value.order_by_keyword

    ASC = OrderByType(1, 'ASC')
    DESC = OrderByType(2, 'DESC')


# JOINS
JoinType = namedtuple('JoinType', ['value', 'join_keyword'])
"""Tupla para propiedades de EnumJoinTypes. La uso para poder añadirle una propiedad al enumerado, aparte del propio
valor."""


class EnumJoinTypes(enum.Enum):
    """Enumerado de tipos de OrderBy."""

    @property
    def join_keyword(self):
        return self.value.join_keyword

    INNER_JOIN = JoinType(1, 'INNER JOIN')
    LEFT_JOIN = JoinType(2, 'LEFT JOIN')


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
    """Clase para modelado de cláusulas WHERE."""

    def __init__(self, field_name: str, filter_type: (EnumFilterTypes, str), object_to_compare: any,
                 operator_type: (EnumOperatorTypes, str) = None, related_filter_clauses: list = None):
        self.field_name = field_name
        """Nombre del campo de la relación, respetando el nivel de anidamiento contando desde la entidad principal sin 
        incluirla, por ejemplo si para el dao de Clientes: tipo_cliente.usuario_creacion sería un join desde Clientes a 
        TipoCliente y de TipoCliente a Usuario."""
        self.filter_type = filter_type if isinstance(filter_type, EnumFilterTypes) else EnumFilterTypes[filter_type]
        """Tipo de filtro."""
        self.object_to_compare = object_to_compare
        """Objeto a comparar."""
        self.operator_type = (operator_type if isinstance(operator_type, EnumOperatorTypes)
                              else EnumOperatorTypes[operator_type]) if operator_type is not None \
            else EnumOperatorTypes.AND
        self.related_filter_clauses = related_filter_clauses
        """Lista de otros FilterClause relacionados con éste. Se utiliza para filtros que van todos juntos 
        dentro de un paréntesis."""


class JoinClause(object):
    """Clase para modelado de cláusulas JOIN."""

    def __init__(self, field_name: str, join_type: (EnumJoinTypes, str), is_join_with_fetch: bool = False):
        self.field_name = field_name
        """Nombre del campo de la relación entre entidades sobre la que se quiere hacer join."""
        self.join_type = join_type if isinstance(join_type, EnumJoinTypes) else EnumJoinTypes[join_type]
        """Tipo de cláusula JOIN."""
        self.is_join_with_fetch = is_join_with_fetch
        """Indica si el join va a traer todos los campos de la tabla."""


class GroupByClause(object):
    """Clase para modelado de cláusulas GROUP BY."""

    def __init__(self, field_name: str):
        self.field_name = field_name
        """Nombre del campo sobre el que se va a aplicar la cláusula group by."""


class OrderByClause(object):
    """Clase para modelado de cláusulas ORDER BY."""

    def __init__(self, field_name: str, order_by_type: (EnumOrderByTypes, str)):
        self.field_name = field_name
        """Nombre del campo."""
        self.order_by_type = order_by_type if isinstance(order_by_type, EnumOrderByTypes) \
            else EnumOrderByTypes[order_by_type]
        """Tipo de cláusula ORDER BY."""
