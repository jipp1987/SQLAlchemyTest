from copy import deepcopy
from typing import List

from core.service.service import BaseService, service_method, ServiceFactory
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl, RolDaoImpl, UsuarioRolDaoImpl
from impl.model.rol import Rol
from impl.model.usuario import Usuario
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

    USUARIOS_ASOCIADOS_DICT_KEY = "usuarios_asociados"
    """Clave para recuperar los usuarios asociados de un json string."""

    def __init__(self):
        super().__init__(dao=RolDaoImpl())

    @service_method
    def update_fields(self, registry_id: any, values_dict: dict):
        # Sobrescritura para recuperar del json los usuarios asociados al rol.
        usuarios_transient = []

        if self.USUARIOS_ASOCIADOS_DICT_KEY in values_dict:
            usuarios_asociados: List[dict] = values_dict[self.USUARIOS_ASOCIADOS_DICT_KEY]
            usuario_rol: UsuarioRol

            for u in usuarios_asociados:
                usuario_rol = UsuarioRol()
                setattr(usuario_rol, "usuario", Usuario(**u["usuario"]))
                setattr(usuario_rol, "usuarioid", usuario_rol.usuario.id)
                setattr(usuario_rol, "rol", Rol(**u["rol"]))
                setattr(usuario_rol, "rolid", usuario_rol.rol.id)
                usuarios_transient.append(usuario_rol)

            # Elimino el valor del diccionario, lo trato individualmente en el update
            values_dict.pop(self.USUARIOS_ASOCIADOS_DICT_KEY)

        registry = self._prepare_entity_for_update_fields(registry_id, values_dict)
        registry.usuarios_transient = usuarios_transient

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
        usuarios_roles: list = deepcopy(registry.usuarios_transient)
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
