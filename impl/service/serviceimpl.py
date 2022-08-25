from copy import deepcopy
from typing import List

from core.dao.modelutils import deserialize_model
from core.service.service import BaseService, service_method, ServiceFactory
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl, RolDaoImpl, UsuarioRolDaoImpl
from impl.model.rol import Rol
from impl.model.usuariorol import UsuarioRol


class ClienteServiceImpl(BaseService):
    """Implementación del service de clientes."""

    def __init__(self):
        super().__init__(dao=ClienteDaoImpl())


class TipoClienteServiceImpl(BaseService):
    """Implementación del service de tipos de cliente."""

    def __init__(self):
        super().__init__(dao=TipoClienteDaoImpl())


class UsuarioServiceImpl(BaseService):
    """Implementación del service de usuarios."""

    def __init__(self):
        super().__init__(dao=UsuarioDaoImpl())


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
    def update_fields(self, registry_id: any, values_dict: dict):
        # Sobrescritura para recuperar del json los usuarios asociados al rol.
        usuarios_roles = []

        if self.USUARIOS_ROLES_DICT_KEY in values_dict:
            usuarios_roles_dict: List[dict] = values_dict[self.USUARIOS_ROLES_DICT_KEY]

            for u in usuarios_roles_dict:
                usuarios_roles.append(deserialize_model(u, UsuarioRol))

            # Elimino el valor del diccionario, lo trato individualmente en el update
            values_dict.pop(self.USUARIOS_ROLES_DICT_KEY)

        registry = self._prepare_entity_for_update_fields(registry_id, values_dict)
        registry.usuarios_roles = usuarios_roles

        self.update(registry)

        return registry

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
