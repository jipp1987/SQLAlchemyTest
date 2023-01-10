from copy import deepcopy
from typing import List

from flask_jwt_extended import create_access_token

from core.dao.daotools import EnumFilterTypes, FilterClause, FieldClause
from core.service.service import BaseService, ServiceFactory
from core.service.servicetools import service_method, ServiceException, EnumServiceExceptionCodes
from core.utils.passwordutils import check_password_using_bcrypt, hash_password_using_bcrypt
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl, RolDaoImpl, UsuarioRolDaoImpl
from impl.model.cliente import Cliente
from impl.model.rol import Rol
from impl.model.tipocliente import TipoCliente
from impl.model.usuario import Usuario


class ClienteServiceImpl(BaseService):
    """Implementación del service de clientes."""

    def __init__(self):
        super().__init__(dao=ClienteDaoImpl())

    @service_method
    def load(self, registry_id: int) -> Rol:
        cliente: Cliente = super().load(registry_id)

        # Carga de tipo de cliente
        tipos_cliente_service = ServiceFactory.get_service(TipoClienteServiceImpl)
        cliente.tipo_cliente = tipos_cliente_service.find_by_id(cliente.tipoclienteid)

        return cliente

    @service_method
    def create(self, entity: Cliente):
        # Sobrescritura para comprobar el código
        if self.does_code_exist("codigo", entity.codigo, None):
            raise ValueError(f"There is already an entity with code: {entity.codigo}.")

        # El código debe tener un tamaño de 10 caracteres y ser consistir únicamente en dígitos
        if not len(entity.codigo) == 10 or not entity.codigo.isdigit():
            raise ValueError(f"Code must have ten characters. Only digits are allowed.")

        super().create(entity)

    @service_method
    def update(self, entity: Cliente):
        # Sobrescritura para comprobar el código
        if self.does_code_exist("codigo", entity.codigo, entity.id):
            raise ValueError(f"There is already an entity with code: {entity.codigo}.")

        # El código debe tener un tamaño de 10 caracteres y ser consistir únicamente en dígitos
        if not len(entity.codigo) == 10 or not entity.codigo.isdigit():
            raise ValueError(f"Code must have ten characters. Only digits are allowed.")

        super().update(entity)


class TipoClienteServiceImpl(BaseService):
    """Implementación del service de tipos de cliente."""

    def __init__(self):
        super().__init__(dao=TipoClienteDaoImpl())

    @service_method
    def create(self, entity: TipoCliente):
        # Sobrescritura para comprobar el código
        if self.does_code_exist("codigo", entity.codigo, None):
            raise ValueError(f"There is already an entity with code: {entity.codigo}.")

        # El código debe tener un tamaño de 10 caracteres y ser consistir únicamente en dígitos
        if not len(entity.codigo) == 4 or not entity.codigo.isdigit():
            raise ValueError(f"Code must have four characters. Only digits are allowed.")

        super().create(entity)

    @service_method
    def update(self, entity: TipoCliente):
        # Sobrescritura para comprobar el código
        if self.does_code_exist("codigo", entity.codigo, entity.id):
            raise ValueError(f"There is already an entity with code: {entity.codigo}.")

        # El código debe tener un tamaño de 10 caracteres y ser consistir únicamente en dígitos
        if not len(entity.codigo) == 4 or not entity.codigo.isdigit():
            raise ValueError(f"Code must have four characters. Only digits are allowed.")

        super().update(entity)


class UsuarioServiceImpl(BaseService):
    """Implementación del service de usuarios."""

    def __init__(self):
        super().__init__(dao=UsuarioDaoImpl())

    @service_method
    def check_password(self, usuario: Usuario):
        """
        Comprueba y establece el valor encriptado del password del usuario si es necesario.
        :param usuario:
        :return: None
        """
        if usuario.password is not None:
            # Si el usuario no tiene id significa que aún no se ha creado en la base de datos, con lo cual se
            # establecer el password encriptado directamente
            if usuario.id is None:
                usuario.password = hash_password_using_bcrypt(usuario.password)
            else:
                # Si tiene id, busco el password antiguo en la base de datos para comprobar si realmente ha cambiado
                # usando el comparador de bcrypt. Dado que bcrypt va a hashear el password de otra forma, no quiero
                # modificar el valor en la base de datos salvo que realmente sea otro password.
                filters: List[FilterClause] = [FilterClause(field_name="id", filter_type=EnumFilterTypes.EQUALS,
                                                            object_to_compare=usuario.id)]
                fields: List[FieldClause] = [FieldClause(field_name="password")]
                result: List[Usuario] = self.select_fields(field_clauses=fields, filter_clauses=filters,
                                                           offset=0, limit=1)

                if result and len(result) > 0:
                    usuario_old = result[0]

                    # Si el password del usuario ya estuviese encriptado en este punto, sería igual que el original
                    if usuario.password != usuario_old.password:
                        if not check_password_using_bcrypt(usuario.password, usuario_old.password):
                            usuario.password = hash_password_using_bcrypt(usuario.password)
                        else:
                            # Mantener el password original en caso contrario (el password del usuario es el mismo
                            # pero está desencriptado)
                            usuario.password = usuario_old.password

    @service_method
    def create(self, entity: Usuario):
        # Sobrescritura de insert para comprobar password
        self.check_password(entity)
        super().create(entity)

    @service_method
    def update(self, entity: Usuario):
        # Sobrescritura de update para comprobar password
        self.check_password(entity)
        super().update(entity)

    @service_method
    def find_user_by_username_and_password(self, username: str, password: str) -> Usuario:
        """
        Devuelve un usuario buscándolo por nombre de usuario y contraseña (sin encriptar) Lanza excepción si no
        lo encuentra.
        :param username: Nombre de usuario
        :param password: Contraseña sin encriptar
        :return: Usuario.
        """
        # Busco el usuario
        filters: List[FilterClause] = [
            FilterClause(field_name="username", filter_type=EnumFilterTypes.EQUALS, object_to_compare=username)
        ]

        usuarios: List[Usuario] = self.select(filter_clauses=filters, limit=1)

        # Si no encuentra el usuario, lanzar excepción
        if not usuarios:
            raise ServiceException(message="User does not exist.", i18n_key=("i18n_auth_error_username", [username]),
                                   error_code=EnumServiceExceptionCodes.AUTHORIZATION_ERROR)

        # Validar password
        if not check_password_using_bcrypt(password, usuarios[0].password):
            raise ServiceException(message="Invalid password.",
                                   error_code=EnumServiceExceptionCodes.AUTHORIZATION_ERROR)

        return usuarios[0]

    @service_method
    def create_token(self, username: str, password: str) -> str:
        """
        Crea un token JWT para el usuario, buscándolo primero por usuario y contraseña (sin encriptar).
        :param username: Nombre de usuario
        :param password: Contraseña sin encriptar
        :return: Token JWT.
        """
        usuario: Usuario = self.find_user_by_username_and_password(username, password)
        # Devuelvo el token JWT
        return create_access_token(identity=usuario.id)


class RolServiceImpl(BaseService):
    """Implementación del service de roles."""

    USUARIOS_ROLES_DICT_KEY = "usuarios_roles"
    """Clave para recuperar los usuarios asociados de un json string."""

    def __init__(self):
        super().__init__(dao=RolDaoImpl())

    @service_method
    def load(self, registry_id: int) -> Rol:
        rol: Rol = super().load(registry_id)

        # Carga de los usuarios_roles
        usuario_rol_service = ServiceFactory.get_service(UsuarioRolServiceImpl)
        rol.usuarios_roles = usuario_rol_service.find_by_rol_id(registry_id)

        return rol

    @service_method
    def update(self, registry) -> None:
        """
        Modifica una entidad en la base de datos. Modifica la entidad al completo, tal y como llega en el parámetro.
        :param registry: Registro a modificar.
        :return: None
        """
        # Hago una copia de los usuarios asociados y vacío la lista para evitar problemas.
        usuarios_roles: list = deepcopy(registry.usuarios_roles)
        registry.usuarios_transient = []

        self._dao.update(registry)

        # Actualizo la relación many-to-many
        if usuarios_roles:
            usuario_rol_service = ServiceFactory.get_service(UsuarioRolServiceImpl)
            usuario_rol_service.update_usuarios_roles_by_rol(registry, usuarios_roles)


class UsuarioRolServiceImpl(BaseService):
    """Implementación del service de roles."""

    def __init__(self):
        super().__init__(dao=UsuarioRolDaoImpl())

    @service_method
    def update_usuarios_roles_by_rol(self, rol, usuarios_asociados):
        """
        Actualiza los usuarios-roles por rol.
        :param rol:
        :param usuarios_asociados:
        :return:
        """
        return self._dao.update_usuarios_roles_by_rol(rol, usuarios_asociados)

    @service_method
    def find_by_rol_id(self, rol_id: int):
        """
        Encuentra los usuarios-roles asociados a un rol.
        :param rol_id:
        :return:
        """
        return self._dao.find_by_rol_id(rol_id)
