from typing import List

from sqlalchemy.orm import contains_eager

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

    def select_all(self) -> List[Cliente]:
        my_session = type(self).get_session_for_current_thread()

        # result: List[Cliente] = my_session.query(Cliente).options(joinedload(Cliente.tipo_cliente)).all()
        result: List[Cliente] = my_session.query(Cliente).join(Cliente.tipo_cliente).\
            options(contains_eager(Cliente.tipo_cliente)).filter(TipoCliente.id == 280).all()

        for r in result:
            print(r)

        # Para evitar problemas, hago flush y libero todos los elementos
        my_session.flush()
        my_session.expunge_all()

        return result
