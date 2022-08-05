from typing import List

from core.service.service import BaseService, service_method
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl
from impl.model.cliente import Cliente


class ClienteServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=ClienteDaoImpl())

    @service_method
    def select_all(self) -> List[Cliente]:
        return self._dao.select_all()


class TipoClienteServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=TipoClienteDaoImpl())


class UsuarioServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=UsuarioDaoImpl())
