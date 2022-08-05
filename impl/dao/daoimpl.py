from core.dao.basedao import BaseDao
from impl.model.cliente import Cliente
from impl.model.tipocliente import TipoCliente
from impl.model.usuario import Usuario


class TipoClienteDaoImpl(BaseDao):
    """Implementación del DAO de tipos de cliente."""

    def __init__(self):
        super().__init__(table=TipoCliente.__tablename__, entity_type=TipoCliente)


class UsuarioDaoImpl(BaseDao):
    """Implementación del DAO de usuarios."""

    def __init__(self):
        super().__init__(table=Usuario.__tablename__, entity_type=Usuario)


class ClienteDaoImpl(BaseDao):
    """Implementación del DAO de clientes."""

    def __init__(self):
        super().__init__(table=Cliente.__tablename__, entity_type=Cliente)
