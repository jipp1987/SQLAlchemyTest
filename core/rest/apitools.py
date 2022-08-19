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

    def __init__(self, username: str = None, password: str = None, action: int = None,
                 select_action: int = None, request_object: any = None):
        super().__init__()
        self.username = username
        """Nombre de usuario para token de autenticación."""
        self.password = password
        """Password de usuario para token de autenticación."""
        self.action = action
        """Acción a realizar."""
        self.select_action = select_action
        """Acción especial de select, por ejemplo un recuento de líneas. Se es None y action es select, 
        sería una consulta normal."""
        self.request_object = request_object
        """Objeto de la request. Puede ser un BaseEntity, una lista de filtros..."""


@dataclass(init=True, frozen=True)
class RequestResponse:
    """Objeto de respuesta de request."""
    success: bool
    status_code: int
    response_object: any
