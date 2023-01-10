import enum
from typing import Union, List, Tuple

from core.utils.i18nutils import translate


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

    def __init__(self, message: str,
                 error_code: Union[EnumServiceExceptionCodes, None] = EnumServiceExceptionCodes.SERVICE_ERROR,
                 i18n_key: Union[str, Tuple[str, Union[List[str], None]]] = None, trace: str = None,
                 source_exception: Exception = None, exception_type: str = None):
        Exception.__init__(self)
        self.message: str = message
        """Mensaje de la excepción."""
        self.error_code: EnumServiceExceptionCodes = error_code
        """Código de la excepción. Por defecto se asume que es un SERVICE_ERROR salvo que se especifique otro tipo. 
        Si es None, se intentará establecer a partir de la excepción origen si existe, en caso contrario será 
        OTHER_ERROR."""
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

        # Si hay excepción origen y no se ha establecido un código de error, intentar calcularlo a partir de
        # la excepción origen
        if self.error_code is None:
            if self.source_exception is not None:
                self.__set_error_code_by_source_exception()
            else:
                self.error_code = EnumServiceExceptionCodes.OTHER_ERROR

    def __str__(self):
        return self.exception_type + f"\n{self.message}" + \
               ("\n\nTrace:\n" + self.trace if self.trace is not None else "")

    def get_translated_message(self, translations: dict, locale: str) -> str:
        """
        Devuelve el mensaje de error traducido en caso de que tenga clave i18n, o bien el mensaje normal
        en caso de que no la tenga.
        :param translations: Diccionario con las traducciones.
        :param locale: Clave iso del idioma objetivo.
        :return: str
        """
        if self.i18n_key is not None:
            if isinstance(self.i18n_key, tuple):
                return translate(key=self.i18n_key[0], languages=translations, locale_iso=locale, args=self.i18n_key[1])
            else:
                # En este caso es string
                return translate(key=self.i18n_key, languages=translations, locale_iso=locale)
        else:
            return self.message

    def __set_error_code_by_source_exception(self) -> None:
        """Establece el código de error en función de la excepción origen."""
        # Habría que ir añadiendo tipos de excepciones aquí e ir estableciendo códigos en función del mismo,
        # de momento lo dejo así.
        self.error_code = EnumServiceExceptionCodes.OTHER_ERROR
