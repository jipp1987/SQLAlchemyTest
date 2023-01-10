import enum
from typing import Union, List, Tuple


def service_method(function):
    """Utilizo este decorador para establecer un método de BaseService como método de transacción
    en la base de datos."""

    def wrapped(*args, **kwargs):  # noqa
        # Esta parte sería para añadir alguna lógica en la función, pero no es el caso. Realmente la quiero devolver
        # como tal pero con un atributo a mayores para saber que le he añadido un decorador "service_function_dec". Lo
        # utilizo para saber qué funciones deben iniciar transacciones de base de datos y hacer rollback en caso de
        # error.
        pass

    # Le puedo añadir a cualquier función un atributo al vuelo, así que le añado uno para saber que tiene etiqueta
    function.is_service_method = True
    return function


class EnumServiceExceptionCodes(enum.Enum):
    """Enumerado de códigos de funciones de agregado."""

    VALUE_ERROR = 1
    AUTHORIZATION_ERROR = 2
    CONNECTION_ERROR = 3
    DUPLICITY_ERROR = 4
    QUERY_ERROR = 5
    SERVICE_ERROR = 6
    OTHER_ERROR = 7


class ServiceException(Exception):
    """Modelo para excepciones de servicio."""

    def __init__(self, message: str, error_code: EnumServiceExceptionCodes = EnumServiceExceptionCodes.SERVICE_ERROR,
                 i18n_key: Union[str, Tuple[str, Union[List[str], None]]] = None, trace: str = None,
                 source_exception: Exception = None, exception_type: str = None):
        Exception.__init__(self)
        self.message: str = message
        """Mensaje de la excepción."""
        self.error_code: EnumServiceExceptionCodes = error_code
        """Código de la excepción. Por defecto se asume que es un SERVICE_ERROR salvo que se especifique otro tipo."""
        self.i18n_key: Union[str, Tuple[str, List[str]]] = i18n_key
        """Clave i18n para internacionalización. Puede ser un string o una tupla con una clave y unos parámetros 
        para sustituir en el string. None por defecto."""
        self.trace: str = trace
        """Traza del error. None por defecto, está pensado para generar una excepción desde otra sin usar '...from e'"""
        self.source_exception: Exception = source_exception
        """Excepción a partir de la que se ha originado la excepción personalizada. Por defecto ServiceException 
        si se pasa None como parámetro."""
        self.exception_type: str = type(self).__name__ if exception_type is None else exception_type
        """Tipo de excepción."""

    def __str__(self):
        return self.exception_type + f"\n{self.message}" + \
               ("\n\nTrace:\n" + self.trace if self.trace is not None else "")
