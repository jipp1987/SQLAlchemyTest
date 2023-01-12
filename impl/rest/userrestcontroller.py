from flask import Blueprint
from flask_jwt_extended import create_access_token, create_refresh_token, verify_jwt_in_request, get_jwt_identity

from core.rest.apitools import RequestResponse, EnumHttpResponseStatusCodes, RequestBody, \
    convert_request_response_to_json_response
from impl.model.usuario import Usuario
from impl.rest import servicehandler
from impl.rest.restutils import rest_fn
from impl.service.serviceimpl import UsuarioServiceImpl

user_service_blueprint = Blueprint("UserService", __name__, url_prefix='/api/UserService')
"""Blueprint para módulo de usuarios."""

_usuario_service: UsuarioServiceImpl = getattr(servicehandler, f"UsuarioRestService")
"""Servicio de usuarios."""


@user_service_blueprint.route('/login', methods=['POST'])
def login():
    """
    Servicio Rest para hacer login.
    """

    @rest_fn
    def __login(*args, **kwargs):  # noqa
        """
        Función interna para obtener un token JWT.
        :param request_body: Debe tener un username y un password.
        :return: Response. La propiedad response_object devuelve un diccionario con dos claves-valor:
        token_jwt y refresh_token.
        """
        request_body: RequestBody = kwargs["request_body"]
        # Obtener username y password del cuerpo de la petición
        username: str = request_body.request_object["username"]
        password: str = request_body.request_object["password"]

        # Obtener usuario
        user: Usuario = _usuario_service.find_user_by_username_and_password(username=username, password=password)

        # Crear token de acceso y token de refrescado y devolverlos en la respuesta
        response_body = RequestResponse(response_object={"token_jwt": create_access_token(identity=user.id),
                                                         "refresh_token": create_refresh_token(identity=user.id)},
                                        success=True, status_code=EnumHttpResponseStatusCodes.OK.value)
        json_response = convert_request_response_to_json_response(response_body)

        return json_response

    return __login()


@user_service_blueprint.route('/refresh_token', methods=['POST'])
def refresh_token():
    """
    Servicio Rest para refrescar el token.
    """

    @rest_fn
    def __refresh_token(*args, **kwargs):  # noqa
        """
        Función interna para refrescar el token JWT.
        :return: Response. La propiedad response_object es un string con el nuevo token.
        """
        # Utilizamos verify_jwt_in_request pasando como parámetro refresh=True para que valide que con la request
        # viene un token de refrescado.
        verify_jwt_in_request(refresh=True)
        # Recuperamos la identidad del usuario desde el contexto de flask y la utilizamos para crear un nuevo
        # token de acceso.
        identity = get_jwt_identity()
        new_refresh_token = create_access_token(identity=identity)

        response_body = RequestResponse(response_object=new_refresh_token, success=True,
                                        status_code=EnumHttpResponseStatusCodes.OK.value)
        json_response = convert_request_response_to_json_response(response_body)

        return json_response

    return __refresh_token()
