import sys
import traceback
from functools import wraps
from typing import Tuple, Union

from flask import request
from flask_jwt_extended import verify_jwt_in_request

from core.dao.modelutils import BaseEntity
from core.rest.apitools import RequestResponse, RequestBody, EnumHttpResponseStatusCodes, DBRequestBody, \
    convert_request_response_to_json_response
from core.service.servicetools import ServiceException, EnumServiceExceptionCodes
from core.utils.i18nutils import translate, prepare_translations
from core.utils.jsonutils import encode_object_to_json, decode_object_from_json
from impl.rest import servicehandler


# Preparar traducciones de la aplicación
_TRANSLATIONS: dict = prepare_translations(language_list=["es_ES", "en_GB"], mo_file_name="base", dir_name="resources")
"""Traducciones i18n."""


def handle_service_exception(e: ServiceException, locale: str) -> Tuple[str, str]:
    """
    Maneja las excepciones de servicio.
    :param e: Excepción de servicio.
    :param locale: Idioma para traducción.
    :return: Mensaje de error.
    """
    # Comprobar el código de error asociado a la excepción
    error: str
    if e.error_code == EnumServiceExceptionCodes.VALUE_ERROR:
        error = translate(key="i18n_error_serviceException_valueError", languages=_TRANSLATIONS, locale_iso=locale)
    elif e.error_code == EnumServiceExceptionCodes.AUTHORIZATION_ERROR:
        error = translate(key="i18n_error_serviceException_authorizationError", languages=_TRANSLATIONS,
                          locale_iso=locale)
    elif e.error_code == EnumServiceExceptionCodes.CONNECTION_ERROR:
        error = translate(key="i18n_error_serviceException_connectionError", languages=_TRANSLATIONS, locale_iso=locale)
    elif e.error_code == EnumServiceExceptionCodes.DUPLICITY_ERROR:
        error = translate(key="i18n_error_serviceException_duplicityError", languages=_TRANSLATIONS, locale_iso=locale)
    elif e.error_code == EnumServiceExceptionCodes.QUERY_ERROR:
        error = translate(key="i18n_error_serviceException_queryError", languages=_TRANSLATIONS, locale_iso=locale)
    elif e.error_code == EnumServiceExceptionCodes.SERVICE_ERROR:
        error = translate(key="i18n_error_serviceException_serviceError", languages=_TRANSLATIONS, locale_iso=locale)
    else:
        error = translate(key="i18n_error_serviceException_otherError", languages=_TRANSLATIONS, locale_iso=locale)

    # Primero obtengo el mensaje de error traducido (si no hay traducción usará devuelve el mensaje normal de la
    # excepción)
    error = f"{error}\n{e.get_translated_message(translations=_TRANSLATIONS, locale=locale)}"

    # Si la excepción tiene una excepción origen, añadir la traza
    trace: str
    if e.source_exception is not None:
        trace = f"{str(e.source_exception)}\n{e.trace}"
    else:
        # Si no tiene excepción origen, la traza es igual al error. Normalmente es para incidencias custom.
        trace: str = error

    return error, trace


def rest_fn(function):
    """
    Decorador para implementaciones de rest controller.
    :param function: Función a ejecutar.
    :return: Decorador
    """

    @wraps(function)
    def decorator(*args, **kwargs):
        locale: str = "en_GB"
        request_error: Union[str, None] = None

        try:
            # Obtengo el objeto enviado por json con la petición
            json_format = encode_object_to_json(request.get_json(force=True))
            # Luego transformo el string json a un objeto RequestBody, pasando el tipo como parámetro
            request_body: RequestBody = decode_object_from_json(json_format, RequestBody)

            # Añado los parámetros a la función
            kwargs['request_body'] = request_body

            return function(*args, **kwargs)
        except ServiceException as s:
            error, trace = handle_service_exception(s, locale)
            print(trace, file=sys.stderr)
            request_error = error
        except Exception as e:
            print(traceback.print_exc())
            request_error = str(e)
        finally:
            if request_error is not None:
                response_body: RequestResponse = RequestResponse(response_object=request_error, success=False,
                                                                 status_code=EnumHttpResponseStatusCodes.BAD_REQUEST.
                                                                 value)
                return convert_request_response_to_json_response(response_body)

    return decorator


def db_rest_fn(function):
    """
    Decorador para tener un cuerpo común para todas las funciones del módulo de bases de datos.
    :param function: Función a ejecutar.
    :return: Decorador
    """

    @wraps(function)
    def decorator(*args, **kwargs):
        locale: str = "en_GB"
        request_error: Union[str, None] = None

        try:
            # id_token = request.headers['Authorization'].split(' ').pop()
            # Verificar que se ha enviado el token de autenticación
            verify_jwt_in_request()
            # claims = get_jwt()

            # Obtengo el objeto enviado por json con la petición
            json_format = encode_object_to_json(request.get_json(force=True))
            # Luego transformo el string json a un objeto RequestBody, pasando el tipo como parámetro
            request_body: DBRequestBody = decode_object_from_json(json_format, DBRequestBody)

            # En función de la entidad seleccionada, cargar el servicio correspodiente
            if request_body.entity is None or not request_body.entity:
                raise ValueError("You have to specify a target entity.")

            try:
                service = getattr(servicehandler, f"{request_body.entity}RestService")
            except AttributeError as e1:
                raise AttributeError(f"Entity {request_body.entity} does not exist.") from e1

            # Añado los parámetros a la función
            kwargs['request_body'] = request_body
            kwargs['service'] = service

            return function(*args, **kwargs)
        except ServiceException as s:
            error, trace = handle_service_exception(s, locale)
            print(trace, file=sys.stderr)
            request_error = error
        except Exception as e:
            print(traceback.print_exc())
            request_error = str(e)
        finally:
            if request_error is not None:
                response_body: RequestResponse = RequestResponse(response_object=request_error, success=False,
                                                                 status_code=EnumHttpResponseStatusCodes.BAD_REQUEST.
                                                                 value)
                return convert_request_response_to_json_response(response_body)

    return decorator


def does_entity_have_create_update_user(entity_type: type(BaseEntity)) -> Tuple[bool, bool]:
    """
    Devuelve una tupla de dos boolean, comprobando si el tipo tiene los atributos "usuario_creacion" y
    "usuario_ult_mod", devolviendo True o False para cada caso en ese orden.
    :param entity_type: BaseEntity.
    :return: Tuple[bool, bool]
    """
    o = entity_type()
    result: Tuple[bool, bool] = (hasattr(o, "usuario_creacion"), hasattr(o, "usuario_ult_mod"))
    return result

