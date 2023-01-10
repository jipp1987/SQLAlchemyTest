import sys
import traceback
import types
from functools import wraps

from core.service.servicetools import ServiceException


def catch_exceptions(function):
    """
    Decorador para capturar las excepciones de las funciones y tratarlas. Lo que hace es envolverlas a CustomException
    pero guardado en ésta la excepción original, su tipo y su traza formateada.
    :param function: Función que va a ser tratada. Se encierra en un trycatch para interpretar la excepción.
    :return: Decorador
    """

    # es un wrapper para funciones
    # lo que defino es un decorador, luego se le pone a las funciones para indicar que deben hacer esta rutina
    @wraps(function)
    def decorator(*args, **kwargs):
        try:
            # Python puede devolver la ejecución de una función
            # Si se produce algún error, uso el código except... para capturarla y tratarla a modo de barrera de fallos
            return function(*args, **kwargs)
        except ServiceException:
            # Si es un ServiceException, lanzar directamente
            raise
        except Exception as e:
            # Caso para otros tipos de excepciones que se hayan podido producir: lo que hago es devolver siempre
            # una ServiceException pero con la información de la excepción original.
            # De esta forma obtengo información de la excepción
            exc_type, exc_instance, exc_traceback = sys.exc_info()

            # Con esto le doy un formato legible a la traza
            formatted_traceback: str = ''.join(traceback.format_tb(exc_traceback))

            # Elimino las dos primeras líneas, se corresponden que el errorhandler y no las quiero en la traza.
            formatted_traceback_split: list = formatted_traceback.split("\n")
            if len(formatted_traceback_split) > 2:
                formatted_traceback = '\n'.join(formatted_traceback_split[2:])

            # Mensaje
            message: str = "An exception of type {0} occurred. Arguments:\n{1!r}". \
                format(type(e).__name__, e.args)

            # Devuelvo una ServiceException
            raise ServiceException(message=message, trace=formatted_traceback,
                                   error_code=None, source_exception=e, exception_type=exc_type.__name__)

    return decorator


class ErrorHandler(type):
    """Metaclase para añadir a funciones de clases una barrera de errores."""

    def __new__(mcs, name, bases, attrs):
        # Recorrer atributos de la clase, buscando aquéllos que sean funciones para asignarles un decorador
        # dinámicamente
        for attr_name, attr_value in attrs.items():
            # si es una función, le añado el decorador
            if isinstance(attr_value, types.FunctionType) or isinstance(attr_value, types.MethodType):  # noqa
                # descarto las funciones heredadas de object, que empiezan y acaban en "__"
                if callable(attr_value) and not attr_name.startswith("__"):
                    # A la función le añado el decorador catch_exceptions
                    attrs[attr_name] = catch_exceptions(attr_value)

        return super(ErrorHandler, mcs).__new__(mcs, name, bases, attrs)
