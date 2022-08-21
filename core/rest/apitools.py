import enum
from dataclasses import dataclass


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

    def __init__(self, username: str = None, password: str = None, request_object: any = None):
        super().__init__()
        self.username = username
        """Nombre de usuario para token de autenticación."""
        self.password = password
        """Password de usuario para token de autenticación."""
        self.request_object = request_object
        """Objeto de la request. Puede ser un BaseEntity, una lista de filtros..."""


class DBRequestBody(RequestBody):
    """Objeto de cuerpo de Request relacionadas con la base de datos."""

    def __init__(self, entity: str, username: str = None, password: str = None, request_object: any = None):
        super().__init__(username=username, password=password, request_object=request_object)
        self.entity = entity
        """Entidad objetivo de la base de datos."""


@dataclass(init=True, frozen=True)
class RequestResponse:
    """Objeto de respuesta de request."""
    success: bool
    status_code: int
    response_object: any
