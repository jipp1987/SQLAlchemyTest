from typing import List

from core.dao.daotools import JoinClause, EnumJoinTypes
from core.service.service import BaseService, service_method, ServiceFactory
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl
from impl.model.tipocliente import TipoCliente


class ClienteServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=ClienteDaoImpl())

    @service_method
    def test(self):
        tipo_cliente_service: TipoClienteServiceImpl = ServiceFactory.get_service(TipoClienteServiceImpl)

        print("\n")
        tipos_clientes = tipo_cliente_service.select()
        for x in tipos_clientes:
            print(x)
        print("\n")

        tipo_cliente = TipoCliente(codigo="7898", descripcion="Prueba 5558")
        tipo_cliente_service.create(tipo_cliente)

        print("\n")
        tipos_clientes = tipo_cliente_service.select()
        for x in tipos_clientes:
            print(x)
        print("\n")

        tipo_cliente_2 = tipo_cliente_service.find_by_id(tipo_cliente.id)

        print(f"\nNuevo tipo de cliente ---> {tipo_cliente_2}\n")

        tipo_cliente_3 = TipoCliente(codigo="2500", descripcion="Prueba 2500")
        tipo_cliente_service.create(tipo_cliente_3)
        tipo_cliente_4 = tipo_cliente_service.find_by_id(tipo_cliente_3.id)

        print(f"\nNuevo tipo de cliente CUATRO ---> {tipo_cliente_4}\n")

        joins: List[JoinClause] = [
            JoinClause(relationship_field_name="tipo_cliente", join_type=EnumJoinTypes.INNER_JOIN,
                       is_join_with_fetch=True)
        ]

        result = self.select(join_clauses=joins)

        for x in result:
            print(x.tipo_cliente)

        if 1 == 1:
            raise Exception("ERROR!!!!!")


class TipoClienteServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=TipoClienteDaoImpl())


class UsuarioServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=UsuarioDaoImpl())
