import enum
from collections import namedtuple
from typing import Union, List


def _find_enum_by_keyword(keyword: str, keyword_field_name: str, enum_type: type(enum.Enum)):
    """
    Busca un enumerado por su keyword.
    :param keyword: Clave a buscar.
    :param keyword_field_name: Nombre del campo a comparar.
    :param enum_type: Tipo de enumerado a comprobar.
    :return: Enumerado del tipo pasado como parámetro. Si no encuentra nada lanza excepción.
    """
    e: enum_type = None

    # Primero intento recuperarlo de los propios valores del enumerado
    names = [member.name for member in enum_type]
    if keyword in names:
        e = enum_type[keyword]
    else:
        # Si no es existe, itero por los valores del enumerado hasta encontrarlo
        for data in enum_type:
            if getattr(data, keyword_field_name) == keyword:
                e = data
                break

    if e is None:
        raise KeyError(f"Not found {keyword} in {enum_type.__name__}.")

    return e


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
    SUM = AggregateFunction(4, 'SUM')
    AVG = AggregateFunction(5, 'AVG')


class FilterClause(object):
    """Clase para modelado de cláusulas WHERE."""

    def __init__(self, field_name: str, filter_type: Union[EnumFilterTypes, str], object_to_compare: any,
                 operator_type: Union[EnumOperatorTypes, str] = None, related_filter_clauses: list = None):
        self.field_name = field_name
        """Nombre del campo de la relación, respetando el nivel de anidamiento contando desde la entidad principal sin 
        incluirla, por ejemplo si para el dao de Clientes: tipo_cliente.usuario_creacion sería un join desde Clientes a 
        TipoCliente y de TipoCliente a Usuario."""
        self.filter_type = filter_type if isinstance(filter_type, EnumFilterTypes) else \
            _find_enum_by_keyword(keyword=filter_type, keyword_field_name="filter_keyword", enum_type=EnumFilterTypes)
        """Tipo de filtro."""
        self.object_to_compare = object_to_compare
        """Objeto a comparar."""
        self.operator_type = (operator_type if isinstance(operator_type, EnumOperatorTypes)
                              else _find_enum_by_keyword(keyword=operator_type, keyword_field_name="operator_keyword",
                                                         enum_type=EnumOperatorTypes)) if operator_type is not None \
            else EnumOperatorTypes.AND
        self.related_filter_clauses = related_filter_clauses
        """Lista de otros FilterClause relacionados con éste. Se utiliza para filtros que van todos juntos 
        dentro de un paréntesis."""


class JoinClause(object):
    """Clase para modelado de cláusulas JOIN."""

    def __init__(self, field_name: str, join_type: Union[EnumJoinTypes, str], is_join_with_fetch: bool = False):
        self.field_name = field_name
        """Nombre del campo de la relación entre entidades sobre la que se quiere hacer join."""
        self.join_type = join_type if isinstance(join_type, EnumJoinTypes) else \
            _find_enum_by_keyword(keyword=join_type, keyword_field_name="join_keyword", enum_type=EnumJoinTypes)
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

    def __init__(self, field_name: str, order_by_type: Union[EnumOrderByTypes, str]):
        self.field_name = field_name
        """Nombre del campo."""
        self.order_by_type = order_by_type if isinstance(order_by_type, EnumOrderByTypes) \
            else _find_enum_by_keyword(keyword=order_by_type, keyword_field_name="order_by_keyword",
                                       enum_type=EnumOrderByTypes)
        """Tipo de cláusula ORDER BY."""


class FieldClause(object):
    """Clase para modelado de selección de campos individuales."""

    def __init__(self, field_name: str, aggregate_function: Union[EnumAggregateFunctions, str] = None):
        self.field_name = field_name
        """Nombre del campo."""
        self.aggregate_function = None if aggregate_function is None else \
            (aggregate_function if isinstance(aggregate_function, EnumAggregateFunctions) else
             _find_enum_by_keyword(keyword=aggregate_function, keyword_field_name="function_keyword",
                                   enum_type=EnumAggregateFunctions))
        """Función de agregado."""


class JsonQuery(object):
    """Clase para el modelado de consultas desde JSON. Contiene distintos tipos de objetos para fabricar la query."""

    # La clase se inicializa a partir de un diccionario, este objeto está pensado para la recepción de filtros desde
    # json
    def __init__(self, query_dict: dict = None):
        # Declaro los campos como None
        self.__filters = None
        self.__order = None
        self.__joins = None
        self.__group_by = None
        self.__fields = None
        self.__offset = None
        self.__limit = None

        # Recorrer diccionario estableciendo valores
        if query_dict is not None:
            for key, value in query_dict.items():
                setattr(self, key, value)

    # PROPIEDADES Y SETTERS
    # Este objeto se crea desde un json: por ello, en el constructor sólo se pasa un diccionario. Uso getters y setters
    # para establecer el valor desde éste y que las propiedades salgan con los tipos que necesito. Lo que hago es usar
    # el operador ** para descomponer cada elemento del listado (que python lo interpreta de json como un diccionario)
    # para que usar pares clave/valor para los argumentos del constructor de cada clase.
    @property
    def filters(self) -> List[FilterClause]:
        """Lista de filtros."""
        return self.__filters

    @filters.setter
    def filters(self, filters):
        if isinstance(filters, list) and filters:
            self.__filters = []
            for f in filters:
                self.__filters.append(FilterClause(**f))

    @property
    def order(self) -> List[OrderByClause]:
        """Lista de order by."""
        return self.__order

    @order.setter
    def order(self, order):
        if isinstance(order, list) and order:
            self.__order = []
            for f in order:
                self.__order.append(OrderByClause(**f))

    @property
    def joins(self) -> List[JoinClause]:
        """Lista de joins."""
        return self.__joins

    @joins.setter
    def joins(self, joins):
        if isinstance(joins, list) and joins:
            self.__joins = []
            for f in joins:
                self.__joins.append(JoinClause(**f))

    @property
    def group_by(self) -> List[GroupByClause]:
        """Lista de group by."""
        return self.__group_by

    @group_by.setter
    def group_by(self, group_by):
        if isinstance(group_by, list) and group_by:
            self.__group_by = []
            for f in group_by:
                self.__group_by.append(GroupByClause(**f))

    @property
    def fields(self) -> List[FieldClause]:
        """Lista de campos del SELECT."""
        return self.__fields

    @fields.setter
    def fields(self, fields):
        if isinstance(fields, list) and fields:
            self.__fields = []
            for f in fields:
                self.__fields.append(FieldClause(**f))

    @property
    def offset(self) -> int:
        """Offset del limit."""
        return self.__offset

    @offset.setter
    def offset(self, offset):
        if isinstance(offset, int):
            self.__offset = offset

    @property
    def limit(self) -> int:
        """Límite de la consulta."""
        return self.__limit

    @limit.setter
    def limit(self, limit):
        if isinstance(limit, int):
            self.__limit = limit
