from typing import List

from core.dao.basedao import BaseDao
from core.dao.daotools import FilterClause, EnumFilterTypes, EnumOperatorTypes, JoinClause, EnumJoinTypes
from core.service.service import ServiceFactory
from core.utils.fileutils import read_section_in_ini_file
from impl.model.tipocliente import TipoCliente
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

    joins: List[JoinClause] = [JoinClause(table_name=TipoCliente, join_type=EnumJoinTypes.INNER_JOIN)]

    # result = service.select(join_clauses=joins)
    result = service.select_all()

    #for r in result:
    #    print(r)


if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    d = read_section_in_ini_file(file_name="db", section="MyDataBase")
    BaseDao.set_db_config_values(**d)

    query_2()
