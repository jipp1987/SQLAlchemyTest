from sqlalchemy import select
from sqlalchemy.orm import aliased, contains_eager

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

    def test_join(self):
        my_session = type(self).get_session_for_current_thread()

        # select cliente, cliente.tipocliente, cliente.tipocliente.usuario_creacion, cliente.tipocliente.usuario_ultmod,
        # , cliente.tipocliente.usuario_ultmod, cliente.usuario_ultmod, cliente.usuario_creacion

        alias_0 = aliased(TipoCliente, name="tipo_cliente")
        alias_1 = aliased(Usuario, name="usuario_creacion")
        alias_2 = aliased(Usuario, name="usuario_ult_mod")

        alias_3 = aliased(Usuario, name="tipo_cliente_usuario_ult_mod")
        alias_4 = aliased(Usuario, name="tipo_cliente_usuario_creacion")

        stmt = select(Cliente).join(Cliente.tipo_cliente.of_type(alias_0)). \
            outerjoin(Cliente.usuario_creacion.of_type(alias_1)). \
            outerjoin(Cliente.usuario_ult_mod.of_type(alias_2)). \
            outerjoin(TipoCliente.usuario_ult_mod.of_type(alias_3)). \
            outerjoin(TipoCliente.usuario_creacion.of_type(alias_4)). \
            options(
            contains_eager(Cliente.tipo_cliente.of_type(alias_0)),
            contains_eager(Cliente.usuario_creacion.of_type(alias_1)),
            contains_eager(Cliente.usuario_ult_mod.of_type(alias_2)),
            contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_ult_mod.of_type(alias_3)),
            contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_creacion.of_type(alias_4))
        ).where(alias_3.username.like("%a%"))

        # Ejecutar la consulta
        result = my_session.execute(stmt).scalars().all()

        # Para evitar problemas, hago flush y libero todos los elementos
        my_session.flush()
        my_session.expunge_all()

        for r in result:
            print(f"\nTipo cliente: {r.tipo_cliente} \n "
                  f"Usuario creación: {r.usuario_creacion}\n "
                  f"Usuario última mod.: {r.usuario_ult_mod}\n"
                  f"TipoCliente - UsuarioCreacion: {r.tipo_cliente.usuario_creacion}\n"
                  f"TipoCliente - usuario_ult_mod: {r.tipo_cliente.usuario_ult_mod}\n"
                  )
