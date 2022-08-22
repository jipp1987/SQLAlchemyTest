import sys
import traceback
from functools import wraps
from typing import List

from flask import Blueprint, make_response, request

from core.dao.daotools import JsonQuery
from core.dao.modelutils import serialize_model, deserialize_model
from core.exception.errorhandler import WrappingException
from core.rest.apitools import RequestResponse, EnumHttpResponseStatusCodes, DBRequestBody
from core.service.service import BaseService
from core.utils.jsonutils import encode_object_to_json, decode_object_from_json
from impl.rest import servicehandler

db_service_blueprint = Blueprint("DBService", __name__, url_prefix='/api/DBService')
"""Blueprint para módulo de api."""


def _db_rest_fn(function):
    """
    Decorador para tener un cuerpo común para todas las funciones del módulo.
    :param function: Función a ejecutar.
    :return: Decorador
    """

    @wraps(function)
    def decorator(*args, **kwargs):
        try:
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
        except (WrappingException, Exception) as e:
            result: str
            # Si hay error conocido, pasarlo en el mensaje de error, sino enviar su representación en forma de string.
            if isinstance(e, WrappingException):
                print(str(e), file=sys.stderr)
                error: str = str(e.source_exception) if e.source_exception is not None else str(e)
                result = error
            else:
                print(traceback.print_exc())
                error: str = str(e)
                result = error

            response_body: RequestResponse = RequestResponse(response_object=result, success=False,
                                                             status_code=EnumHttpResponseStatusCodes.BAD_REQUEST.value)
            return _convert_request_response_to_json_response(response_body)

    return decorator


def _convert_request_response_to_json_response(response_body: RequestResponse):
    """
    Crea una respuesta json a partir de un RequestResponse.
    :param response_body: Objeto RequestResponse
    :return: Respuesta válida para el solicitante en formato json.
    """
    return make_response(encode_object_to_json(response_body), response_body.status_code)


@db_service_blueprint.route('/delete', methods=['POST'])
def delete():

    @_db_rest_fn
    def __delete(*args, **kwargs): # noqa
        """
        Función interna delete.
        :param request_body:
        :param service:
        :return:
        """
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        # Objeto query_object creado a partir del request_object
        entity_to_delete = deserialize_model(request_body.request_object, service.get_entity_type())
        service.delete(entity_to_delete)

        json_result = f"'{entity_to_delete}' has been deleted."
        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)

        return _convert_request_response_to_json_response(response_body)

    return __delete()


@db_service_blueprint.route('/select', methods=['POST'])
def select():

    @_db_rest_fn
    def __select(*args, **kwargs): # noqa
        """
        Función interior select.
        :param request_body:
        :param service:
        :return: Response.
        """
        # En realidad estas dos variables podrían ser los parámetros de la función... pero si no utilizo
        # "*args, **kwargs" el debugger no sabe llegar hasta aquí sin hacer step into desde el decorador.
        request_body: DBRequestBody = kwargs["request_body"]
        service: BaseService = kwargs["service"]

        result: list

        # Consulta
        json_result: List[dict] = []

        # Objeto query_object creado a partir del request_object
        query_object = JsonQuery(request_body.request_object)

        # Si la consulta ha llegado con field_clauses, es una selección de campos individuales. Si no ha llegado con
        # field_clauses, es una selección de entidades.
        if query_object.fields:
            result = service.select_fields(filter_clauses=query_object.filters, order_by_clauses=query_object.order,
                                           join_clauses=query_object.joins,
                                           field_clauses=query_object.fields, group_by_clauses=query_object.group_by,
                                           limit=query_object.limit, offset=query_object.offset)
            if result:
                json_result.extend(result)
        else:
            result = service.select(filter_clauses=query_object.filters, order_by_clauses=query_object.order,
                                    join_clauses=query_object.joins, limit=query_object.limit,
                                    offset=query_object.offset)
            # en este caso, el resultado hay que serializarlo al ser modelos de la base de datos
            if result:
                for r in result:
                    json_result.append(serialize_model(r))

        response_body: RequestResponse = RequestResponse(response_object=json_result, success=True,
                                                         status_code=EnumHttpResponseStatusCodes.OK.value)

        return _convert_request_response_to_json_response(response_body)

    return __select()
