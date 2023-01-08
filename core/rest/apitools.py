import enum
from dataclasses import dataclass

from flask import make_response

from core.utils.jsonutils import encode_object_to_json


class EnumHttpResponseStatusCodes(enum.Enum):
    """Enumerado de códigos de estado para respuestas de peticiones Http."""

    OK = 200
    """OK."""

    CREATED = 201
    """Creado."""

    BAD_REQUEST = 400
    """Algún error en los datos enviados para realizar la petición."""

    UNAUTHORIZED = 401
    """Sin autorización."""

    FORBIDDEN = 403
    """Prohibido."""

    NOT_FOUND = 404
    """Recurso no encontrado."""

    METHOD_NOT_FOUND = 405
    """Método no encontrado."""

    REQUEST_TIMEOUT = 408
    """Tiempo para ejecutar la petición consumido."""

    SERVER_ERROR = 500
    """Error de servidor."""


class RequestBody(object):
    """Objeto de cuerpo de Request."""

    def __init__(self, request_object: any = None):
        super().__init__()
        self.request_object = request_object
        """Objeto de la request. Puede ser un BaseEntity, una lista de filtros..."""


class DBRequestBody(RequestBody):
    """Objeto de cuerpo de Request relacionadas con la base de datos."""

    def __init__(self, entity: str, request_object: any = None):
        super().__init__(request_object=request_object)
        self.entity = entity
        """Entidad objetivo de la base de datos."""


@dataclass(init=True, frozen=True)
class RequestResponse:
    """Objeto de respuesta de request."""
    success: bool
    status_code: int
    response_object: any


def convert_request_response_to_json_response(response_body: RequestResponse):
    """
    Crea una respuesta json a partir de un RequestResponse.
    :param response_body: Objeto RequestResponse
    :return: Respuesta válida para el solicitante en formato json.
    """
    return make_response(encode_object_to_json(response_body), response_body.status_code)
