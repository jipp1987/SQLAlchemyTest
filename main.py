from core.dao.basedao import BaseDao
from core.service.service import ServiceFactory
from core.utils.fileutils import read_section_in_ini_file
from impl.model.tipocliente import TipoCliente
from impl.service.serviceimpl import TipoClienteServiceImpl

if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    d = read_section_in_ini_file(file_name="db", section="MyDataBase")
    BaseDao.set_db_config_values(**d)

    service = ServiceFactory.get_service(TipoClienteServiceImpl)
    result = service.select(filter_clause=TipoCliente.codigo == '0000')
    print(result)
