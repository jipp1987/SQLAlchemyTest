from typing import List

from sqlalchemy import select, and_, or_, delete, insert
from sqlalchemy.orm import aliased, contains_eager
from sqlalchemy.sql import expression

from core.dao.basedao import BaseDao
from core.dao.daotools import FilterClause, EnumFilterTypes, JoinClause, EnumJoinTypes
from impl.model.cliente import Cliente
from impl.model.rol import Rol
from impl.model.tipocliente import TipoCliente
from impl.model.usuario import Usuario
from impl.model.usuariorol import UsuarioRol


class TipoClienteDaoImpl(BaseDao):
    """Implementación del DAO de tipos de cliente."""

    def __init__(self):
        super().__init__(table=TipoCliente.__tablename__, entity_type=TipoCliente)


class UsuarioDaoImpl(BaseDao):
    """Implementación del DAO de usuarios."""

    def __init__(self):
        super().__init__(table=Usuario.__tablename__, entity_type=Usuario)


class RolDaoImpl(BaseDao):
    """Implementación del DAO de usuarios."""

    def __init__(self):
        super().__init__(table=Rol.__tablename__, entity_type=Rol)


class UsuarioRolDaoImpl(BaseDao):
    """Implementación del DAO de usuarios."""

    def __init__(self):
        super().__init__(table=UsuarioRol.__tablename__, entity_type=UsuarioRol)

    def find_by_rol_id(self, rol_id: int):
        join_clauses: List[JoinClause] = [
            JoinClause("rol", EnumJoinTypes.INNER_JOIN),
            JoinClause("usuario", EnumJoinTypes.INNER_JOIN)
        ]

        filters: List[FilterClause] = [
            FilterClause(field_name="rol.id", filter_type=EnumFilterTypes.EQUALS, object_to_compare=rol_id)
        ]

        return self.select(filter_clauses=filters, join_clauses=join_clauses)

    def delete(self, registry: UsuarioRol):
        """
        Sobrescritura de delete.
        :param registry:
        :return:
        """
        # Como la primary key es compuesta, no puedo utilizar la función genérica porque asume que sólo hay una
        # primary key; lo que hago es ejecutar una expresión.
        stmt: expression = delete(UsuarioRol).where(UsuarioRol.rolid == registry.rolid).\
            where(UsuarioRol.usuarioid == registry.usuarioid)
        self._execute_statement(stmt)

    def create(self, registry: UsuarioRol):
        """
        Sobrescritura de delete.
        :param registry:
        :return:
        """
        stmt: expression = insert(UsuarioRol).values(usuarioid=registry.usuarioid, rolid=registry.rolid)
        self._execute_statement(stmt)

    def update_usuarios_roles_by_rol(self, rol: Rol, usuarios_asociados: List[UsuarioRol]):
        """
        Actualiza los registros de usuarios_roles a partir del rol.
        :param rol:
        :param usuarios_asociados:
        :return:
        """
        usuarios_roles_old: List[UsuarioRol] = self.find_by_rol_id(rol.id)

        # Comparo listas para saber qué debo eliminar o crear
        is_exists: bool

        for u in usuarios_roles_old:
            is_exists = False
            for u_new in usuarios_asociados:
                if u == u_new:
                    is_exists = True
                    break

            if not is_exists:
                self.delete(u)

        for u_new in usuarios_asociados:
            is_exists = False
            for u in usuarios_roles_old:
                if u == u_new:
                    is_exists = True
                    break

            if not is_exists:
                self.create(u_new)


class ClienteDaoImpl(BaseDao):
    """Implementación del DAO de clientes."""

    def __init__(self):
        super().__init__(table=Cliente.__tablename__, entity_type=Cliente)

    def test_join(self):
        my_session = type(self).get_session_for_current_thread()

        # select cliente, cliente.tipocliente, cliente.tipocliente.usuario_creacion, cliente.tipocliente.usuario_ultmod,
        # , cliente.tipocliente.usuario_ultmod, cliente.usuario_ultmod, cliente.usuario_creacion from cliente inner
        # join tipo_cliente left join cliente.usuario_creacion left join cliente.usuario_ult_mod
        # left join cliente.tipo_cliente.usuario_creacion left join cliente.tipo_cliente.usuario_ult_mod

        # where cliente.tipo_cliente.codigo like '%0%' and cliente.tipo_cliente.descripcion like '%a%' or
        # (cliente.tipo_cliente.usuario_creacion.username like '%a%' or
        # cliente.tipo_cliente.usuario_creacion.username like '%e%')

        # where(or_(and_(alias_0.codigo.like("%0%"), alias_0.descripcion.like("%a%")),
        #          or_(alias_4.username.like("%a%"), alias_4.username.like("%e%")).self_group()))

        alias_0 = aliased(TipoCliente, name="tipo_cliente")
        alias_1 = aliased(Usuario, name="usuario_creacion")
        alias_2 = aliased(Usuario, name="usuario_ult_mod")

        alias_3 = aliased(Usuario, name="tipo_cliente_usuario_ult_mod")
        alias_4 = aliased(Usuario, name="tipo_cliente_usuario_creacion")

        stmt = select(Cliente).\
            outerjoin(Cliente.usuario_ult_mod.of_type(alias_2)). \
            outerjoin(Cliente.usuario_creacion.of_type(alias_1)). \
            join(Cliente.tipo_cliente.of_type(alias_0)). \
            outerjoin(TipoCliente.usuario_creacion.of_type(alias_4)). \
            outerjoin(TipoCliente.usuario_ult_mod.of_type(alias_3)). \
            options(
            contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_ult_mod.of_type(alias_3)),
            contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_creacion.of_type(alias_4)),
            contains_eager(Cliente.tipo_cliente.of_type(alias_0)),
            contains_eager(Cliente.usuario_creacion.of_type(alias_1)),
            contains_eager(Cliente.usuario_ult_mod.of_type(alias_2)),
        ).where(or_(and_(alias_0.codigo.like("%0%"), alias_0.descripcion.like("%a%")),
                    or_(alias_4.username.like("%a%"), alias_4.username.like("%e%")).self_group()))

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
