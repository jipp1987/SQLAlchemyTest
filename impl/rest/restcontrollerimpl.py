import sys
import traceback
from typing import List

from flask import Blueprint, make_response

from core.dao.daotools import FilterClause, EnumFilterTypes, OrderByClause, EnumOrderByTypes, JoinClause, EnumJoinTypes
from core.dao.modelutils import serialize_model
from core.exception.errorhandler import WrappingException
from core.rest.apitools import RequestResponse, EnumHttpResponseStatusCodes
from core.service.service import ServiceFactory
from core.utils.jsonutils import encode_object_to_json
from impl.service.serviceimpl import TipoClienteServiceImpl

tipo_cliente_blueprint = Blueprint("TipoCliente", __name__, url_prefix='/TipoCliente')
"""Blueprint para módulo de api."""

_service: TipoClienteServiceImpl = ServiceFactory.get_service(TipoClienteServiceImpl)
"""Servicio."""


def _convert_request_response_to_json_response(response_body: RequestResponse):
    """
    Crea una respuesta json a partir de un RequestResponse.
    :param response_body: Objeto RequestResponse
    :return: Respuesta válida para el solicitante en formato json.
    """
    return make_response(encode_object_to_json(response_body), response_body.status_code)


@tipo_cliente_blueprint.route('/select', methods=['POST'])
def select():
    try:
        filters: List[FilterClause] = [FilterClause(field_name="descripcion", filter_type=EnumFilterTypes.LIKE,
                                                    object_to_compare="e")]
        order: List[OrderByClause] = [OrderByClause(field_name="codigo", order_by_type=EnumOrderByTypes.ASC)]

        joins: List[JoinClause] = [
            JoinClause(field_name="usuario_creacion", join_type=EnumJoinTypes.LEFT_JOIN, is_join_with_fetch=True),
            # JoinClause(field_name="usuario_ult_mod", join_type=EnumJoinTypes.LEFT_JOIN, is_join_with_fetch=True)
        ]

        result = _service.select(filter_clauses=filters, order_by_clauses=order, join_clauses=joins)

        json_result = []
        for r in result:
            json_result.append(serialize_model(r))

        # Devuelvo un response_body
        response_body = RequestResponse(response_object=json_result, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)
        return _convert_request_response_to_json_response(response_body)
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

        return _convert_request_response_to_json_response(response_body)
