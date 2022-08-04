from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship

from core.dao.basedao import BaseEntity
from impl.model.tipocliente import TipoCliente
from impl.model.usuario import Usuario


class Cliente(BaseEntity):
    """Modelo de clientes."""

    __tablename__ = 'clientes'

    # Mapeo de columnas de la base de datos
    id = Column(Integer, primary_key=True)
    codigo = Column(String, nullable=False)
    nombre = Column(String, nullable=False)
    apellidos = Column(String, nullable=True)
    saldo = Column(Float, nullable=False)

    usuariocreacionid = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    usuarioultmodid = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    tipoclienteid = Column(Integer, ForeignKey('tiposcliente.id'), nullable=True)

    # Relaciones con otras tablas
    usuario_creacion = relationship(Usuario.__name__, foreign_keys=[usuariocreacionid])
    usuario_ult_mod = relationship(Usuario.__name__, foreign_keys=[usuarioultmodid])
    tipo_cliente = relationship(TipoCliente.__name__, foreign_keys=[tipoclienteid])

    # Constructor
    def __init__(self, **kwargs):
        super(Cliente, self).__init__(**kwargs)

    # MÉTODOS
    @classmethod
    def get_id_field_name(cls):
        """
        Devuelve el nombre del campo de la primary key.
        :return:
        """
        return "id"

    # equals: uso el id para saber si es el mismo tipo de cliente
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        else:
            return False

    # not equals
    def __ne__(self, other):
        return not self.__eq__(other)

    # hashcode
    def __hash__(self):
        # En este caso uso el mismo atributo que para el hash
        return hash(self.id)

    # tostring
    def __repr__(self):
        return f'id = {self.id}, codigo = {self.codigo}, nombre = {self.nombre}, apellidos = {self.apellidos}, ' \
               f'tipo_cliente = {self.tipo_cliente.id if self.tipo_cliente is not None else ""}'
