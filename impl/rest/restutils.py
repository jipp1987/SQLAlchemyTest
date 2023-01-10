import sys
import traceback
from functools import wraps

from flask import request
from flask_jwt_extended import verify_jwt_in_request

from core.rest.apitools import RequestResponse, RequestBody, EnumHttpResponseStatusCodes, DBRequestBody, \
    convert_request_response_to_json_response
from core.service.servicetools import ServiceException
from core.utils.jsonutils import encode_object_to_json, decode_object_from_json
from impl.rest import servicehandler


def rest_fn(function):
    """
    Decorador para implementaciones de rest controller.
    :param function: Función a ejecutar.
    :return: Decorador
    """

    @wraps(function)
    def decorator(*args, **kwargs):
        try:
            # Obtengo el objeto enviado por json con la petición
            json_format = encode_object_to_json(request.get_json(force=True))
            # Luego transformo el string json a un objeto RequestBody, pasando el tipo como parámetro
            request_body: RequestBody = decode_object_from_json(json_format, RequestBody)

            # Añado los parámetros a la función
            kwargs['request_body'] = request_body

            return function(*args, **kwargs)
        except (ServiceException, Exception) as e:
            result: str
            # Si hay error conocido, pasarlo en el mensaje de error, sino enviar su representación en forma de string.
            if isinstance(e, ServiceException):
                print(str(e), file=sys.stderr)
                error: str = str(e.source_exception) if e.source_exception is not None else str(e)
                result = error
            else:
                print(traceback.print_exc())
                error: str = str(e)
                result = error

            response_body: RequestResponse = RequestResponse(response_object=result, success=False,
                                                             status_code=EnumHttpResponseStatusCodes.BAD_REQUEST.value)
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
        except (ServiceException, Exception) as e:
            result: str
            # Si hay error conocido, pasarlo en el mensaje de error, sino enviar su representación en forma de string.
            if isinstance(e, ServiceException):
                print(str(e), file=sys.stderr)
                error: str = str(e.source_exception) if e.source_exception is not None else str(e)
                result = error
            else:
                print(traceback.print_exc())
                error: str = str(e)
                result = error

            response_body: RequestResponse = RequestResponse(response_object=result, success=False,
                                                             status_code=EnumHttpResponseStatusCodes.BAD_REQUEST.value)
            return convert_request_response_to_json_response(response_body)

    return decorator
