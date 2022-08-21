import sys
import traceback
from typing import List, Union

from flask import Blueprint, make_response, request

from core.dao.daotools import JsonQuery
from core.dao.modelutils import serialize_model, BaseEntity
from core.exception.errorhandler import WrappingException
from core.rest.apitools import RequestResponse, EnumHttpResponseStatusCodes, RequestBody
from core.utils.jsonutils import encode_object_to_json, decode_object_from_json
from impl.rest import servicehandler

db_service_blueprint = Blueprint("TipoCliente", __name__, url_prefix='/api/DBService')
"""Blueprint para módulo de api."""


def _convert_request_response_to_json_response(response_body: RequestResponse):
    """
    Crea una respuesta json a partir de un RequestResponse.
    :param response_body: Objeto RequestResponse
    :return: Respuesta válida para el solicitante en formato json.
    """
    return make_response(encode_object_to_json(response_body), response_body.status_code)


@db_service_blueprint.route('/select', methods=['POST'])
def select():
    result: Union[List[dict], List[BaseEntity], str]
    response_body: Union[RequestResponse, None] = None

    try:
        if request.method != 'POST':
            raise ValueError(f"Request method {request.method} not allowed.")

        # Obtengo el objeto enviado por json con la petición
        json_format = encode_object_to_json(request.get_json(force=True))
        # Luego transformo el string json a un objeto RequestBody, pasando el tipo como parámetro
        request_body: RequestBody = decode_object_from_json(json_format, RequestBody)
        # Objeto query_object creado a partir del request_object
        query_object = JsonQuery(request_body.request_object)

        # En función de la entidad seleccionada, cargar el servicio correspodiente
        if query_object.entity is None or not query_object.entity:
            raise ValueError("You have to specify a target entity.")

        try:
            service = getattr(servicehandler, f"{query_object.entity}RestService")
        except AttributeError as e1:
            raise AttributeError(f"Entity {query_object.entity} does not exist.") from e1

        # Consulta
        json_result: List[dict] = []

        # Si la consulta ha llegado con field_clauses, es una selección de campos individuales. Si no ha llegado con
        # field_clauses, es una selección de entidades.
        if query_object.fields:
            result = service.select_fields(filter_clauses=query_object.filters, order_by_clauses=query_object.order,
                                           join_clauses=query_object.joins,
                                           field_clauses=query_object.fields, group_by_clauses=query_object.group_by,
                                           limit=query_object.limit, offset=query_object.offset)
            # select_fields devuelve una lista de diccionarios; los añado directamente al resultado.
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

        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)
    except (WrappingException, Exception) as e:
        # Si hay error conocido, pasarlo en el mensaje de error, sino enviar su representación en forma de string.
        if isinstance(e, WrappingException):
            print(str(e), file=sys.stderr)
            error: str = str(e.source_exception) if e.source_exception is not None else str(e)
            result = error
        else:
            print(traceback.print_exc())
            error: str = str(e)
            result = error

        response_body = RequestResponse(response_object=result, success=False,
                                        status_code=EnumHttpResponseStatusCodes.BAD_REQUEST.value)
    finally:
        return _convert_request_response_to_json_response(response_body)
