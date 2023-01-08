from flask import Blueprint

from core.rest.apitools import RequestResponse, EnumHttpResponseStatusCodes, RequestBody, \
    convert_request_response_to_json_response
from impl.rest import servicehandler
from impl.rest.restutils import rest_fn
from impl.service.serviceimpl import UsuarioServiceImpl

user_service_blueprint = Blueprint("UserService", __name__, url_prefix='/api/UserService')
"""Blueprint para módulo de usuarios."""

_usuario_service: UsuarioServiceImpl = getattr(servicehandler, f"UsuarioRestService")
"""Servicio de usuarios."""


@user_service_blueprint.route('/create_token', methods=['POST'])
def create_token():
    """
    Servicio Rest para solicitar tokens de autenticación.
    """

    @rest_fn
    def __create_token(*args, **kwargs): # noqa
        """
        Función interna para obtener un token JWT.
        :param request_body:
        :param service:
        :return: Response.
        """
        request_body: RequestBody = kwargs["request_body"]
        # Obtener username y password del cuerpo de la petición
        username: str = request_body.request_object["username"]
        password: str = request_body.request_object["password"]

        # Crear token y devolverlo en la respuesta
        token_jwt: str = _usuario_service.create_token(username=username, password=password)
        response_body = RequestResponse(response_object=token_jwt, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)

        return convert_request_response_to_json_response(response_body)

    return __create_token()
