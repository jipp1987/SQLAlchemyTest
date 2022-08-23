from typing import List

from core.dao.daotools import JoinClause, EnumJoinTypes, FieldClause, EnumAggregateFunctions, GroupByClause
from core.service.service import BaseService, service_method, ServiceFactory
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl, RolDaoImpl, UsuarioRolDaoImpl
from impl.model.tipocliente import TipoCliente
from impl.model.usuario import Usuario


class ClienteServiceImpl(BaseService):
    """Implementación del service de clientes."""

    def __init__(self):
        super().__init__(dao=ClienteDaoImpl())

    @service_method
    def test_join(self):
        return self._dao.test_join()

    @service_method
    def test(self):
        tipo_cliente_service: TipoClienteServiceImpl = ServiceFactory.get_service(TipoClienteServiceImpl)

        print("\n")
        tipos_clientes = tipo_cliente_service.select()
        for x in tipos_clientes:
            print(x)
        print("\n")

        tipo_cliente = TipoCliente(codigo="7869", descripcion="Prueba 5558")
        tipo_cliente_service.create(tipo_cliente)

        print("\n")
        tipos_clientes = tipo_cliente_service.select()
        for x in tipos_clientes:
            print(x)
        print("\n")

        tipo_cliente_2 = tipo_cliente_service.find_by_id(tipo_cliente.id)

        print(f"\nNuevo tipo de cliente ---> {tipo_cliente_2}\n")

        tipo_cliente_3 = TipoCliente(codigo="2503", descripcion="Prueba 2500")
        tipo_cliente_service.create(tipo_cliente_3)
        tipo_cliente_4 = tipo_cliente_service.find_by_id(tipo_cliente_3.id)

        print(f"\nNuevo tipo de cliente CUATRO ---> {tipo_cliente_4}\n")

        joins: List[JoinClause] = [
            JoinClause(field_name="tipo_cliente", join_type=EnumJoinTypes.INNER_JOIN,
                       is_join_with_fetch=True)
        ]

        result = self.select(join_clauses=joins)

        for x in result:
            print(x.tipo_cliente)

        tipo_cliente_4.descripcion = "Prueba update"
        tipo_cliente_service.update(tipo_cliente_4)

        tipo_cliente_5 = tipo_cliente_service.find_by_id(tipo_cliente_4.id)
        print(tipo_cliente_5)

        tipo_cliente_service.delete(tipo_cliente)
        tipo_cliente_service.delete(tipo_cliente_3)
        tipo_cliente_service.delete(tipo_cliente_5)

    @service_method
    def test_select_fields(self):
        field_clauses: List[FieldClause] = [
            FieldClause("tipo_cliente.codigo"),
            FieldClause("id", EnumAggregateFunctions.COUNT)
        ]

        join_clauses: List[JoinClause] = [JoinClause("tipo_cliente", EnumJoinTypes.INNER_JOIN)]

        group_by_clauses: List[GroupByClause] = [GroupByClause("tipo_cliente.codigo")]

        result = self._dao.select_fields(field_clauses=field_clauses, join_clauses=join_clauses,
                                         group_by_clauses=group_by_clauses)
        for r in result:
            print(r)


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

    USUARIOS_ASOCIADOS_DICT_KEY = "usuarios"
    """Clave para recuperar los usuarios asociados de un json string."""

    def __init__(self):
        super().__init__(dao=RolDaoImpl())

    @service_method
    def update_fields(self, registry_id: any, values_dict: dict):
        many_to_many_updates = {}

        # Sobrescritura para recuperar del json los usuarios asociados al rol.
        if self.USUARIOS_ASOCIADOS_DICT_KEY in values_dict:
            usuarios_asociados: List[dict] = values_dict[self.USUARIOS_ASOCIADOS_DICT_KEY]
            many_to_many_updates[self.USUARIOS_ASOCIADOS_DICT_KEY] = []
            for u in usuarios_asociados:
                many_to_many_updates[self.USUARIOS_ASOCIADOS_DICT_KEY].append(Usuario(**u))

            # Elimino el valor del diccionario, lo trato individualmente en el update
            values_dict.pop(self.USUARIOS_ASOCIADOS_DICT_KEY)

        return super().update_fields(registry_id, values_dict, many_to_many_updates)

    @service_method
    def update(self, registry, many_to_many_updates: dict = None) -> None:
        """
        Modifica una entidad en la base de datos. Modifica la entidad al completo, tal y como llega en el parámetro.
        :param registry: Registro a modificar.
        :param many_to_many_updates: Diccionario opcional para actualizar relaciones many to many asociadas
        a la entidad.
        :return: None
        """
        self._dao.update(registry)

        # Puede haber llegado una actualización many-to-many
        if many_to_many_updates:
            usuario_rol_service = ServiceFactory.get_service(UsuarioRolServiceImpl)
            usuario_rol_service.update_usuarios_roles_by_rol(registry,
                                                             many_to_many_updates[self.USUARIOS_ASOCIADOS_DICT_KEY])


class UsuarioRolServiceImpl(BaseService):
    """Implementación del service de roles."""

    def __init__(self):
        super().__init__(dao=UsuarioRolDaoImpl())

    @service_method
    def update_usuarios_roles_by_rol(self, rol, usuarios_asociados):
        return self._dao.update_usuarios_roles_by_rol(rol, usuarios_asociados)
