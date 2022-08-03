from core.service.service import BaseService
from impl.dao.daoimpl import ClienteDaoImpl, TipoClienteDaoImpl, UsuarioDaoImpl


class ClienteServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=ClienteDaoImpl())


class TipoClienteServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=TipoClienteDaoImpl())


class UsuarioServiceImpl(BaseService):
    """Implementación del dao de clientes."""

    def __init__(self):
        super().__init__(dao=UsuarioDaoImpl())
