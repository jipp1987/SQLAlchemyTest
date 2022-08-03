from core.dao.basedao import BaseDao
from core.exception.errorhandler import CustomException
from core.service.service import ServiceFactory
from core.utils.fileutils import read_section_in_ini_file
from impl.model.tipocliente import TipoCliente
from impl.service.serviceimpl import TipoClienteServiceImpl

if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    d = read_section_in_ini_file(file_name="db", section="MyDataBase")
    BaseDao.set_db_config_values(**d)

    service = ServiceFactory.get_service(TipoClienteServiceImpl)

    nuevo = TipoCliente(codigo="0000", descripcion="Genérico")

    try:
        service.create(nuevo)
        print(nuevo)
    except CustomException as e1:
        print(e1.line)
    except Exception as e2:
        print(e2)
