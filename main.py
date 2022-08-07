from typing import List

from core.dao.basedao import BaseDao
from core.dao.daotools import FilterClause, EnumFilterTypes, EnumOperatorTypes, JoinClause, EnumJoinTypes
from core.service.service import ServiceFactory
from core.utils.fileutils import read_section_in_ini_file
from impl.service.serviceimpl import TipoClienteServiceImpl, ClienteServiceImpl


def query_1():
    service = ServiceFactory.get_service(TipoClienteServiceImpl)

    filter_1 = FilterClause(field_name="codigo", filter_type=EnumFilterTypes.LIKE, object_to_compare="0")

    filter_2 = FilterClause(field_name="descripcion", filter_type=EnumFilterTypes.LIKE, object_to_compare="iv",
                            operator_type=EnumOperatorTypes.OR)
    filter_3 = FilterClause(field_name="descripcion", filter_type=EnumFilterTypes.LIKE, object_to_compare="rr")
    filter_4 = FilterClause(field_name="descripcion", filter_type=EnumFilterTypes.EQUALS, object_to_compare="Genérico")
    filter_2.related_filter_clauses = [filter_3, filter_4]

    filters: List[FilterClause] = [filter_1, filter_2]

    result = service.select(filter_clauses=filters)

    for r in result:
        print(r)


def query_2():
    service = ServiceFactory.get_service(ClienteServiceImpl)

    joins: List[JoinClause] = [
        JoinClause(relationship_field_name="tipo_cliente.usuario_creacion", join_type=EnumJoinTypes.LEFT_JOIN,
                   is_join_with_fetch=True),
        JoinClause(relationship_field_name="tipo_cliente.usuario_ult_mod", join_type=EnumJoinTypes.LEFT_JOIN,
                   is_join_with_fetch=True),
        JoinClause(relationship_field_name="usuario_ult_mod", join_type=EnumJoinTypes.LEFT_JOIN,
                   is_join_with_fetch=True),
        JoinClause(relationship_field_name="usuario_creacion", join_type=EnumJoinTypes.LEFT_JOIN,
                   is_join_with_fetch=True),
        JoinClause(relationship_field_name="tipo_cliente", join_type=EnumJoinTypes.INNER_JOIN,
                   is_join_with_fetch=True)
    ]

    filters: List[FilterClause] = [
        FilterClause(field_name="tipo_cliente.usuario_creacion.username", filter_type=EnumFilterTypes.LIKE,
                     object_to_compare="q")
    ]

    result = service.select(join_clauses=joins, filter_clauses=filters)

    for r in result:
        print(f"\nTipo cliente: {r.tipo_cliente} \n "
              f"Usuario creación: {r.usuario_creacion}\n "
              f"Usuario última mod.: {r.usuario_ult_mod}\n"
              f"TipoCliente - UsuarioCreacion: {r.tipo_cliente.usuario_creacion}\n"
              f"TipoCliente - usuario_ult_mod: {r.tipo_cliente.usuario_ult_mod}\n"
              )


def query_3():
    service = ServiceFactory.get_service(ClienteServiceImpl)
    service.test_join()


if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    d = read_section_in_ini_file(file_name="db", section="MyDataBase")
    BaseDao.set_db_config_values(**d)

    query_2()
