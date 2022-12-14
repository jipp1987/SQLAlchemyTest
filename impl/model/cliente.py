from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship

from core.dao.modelutils import BaseEntity
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
    tipoclienteid = Column(Integer, ForeignKey('tiposcliente.id'), nullable=False)

    # Relaciones con otras tablas
    usuario_creacion = relationship(Usuario.__name__, foreign_keys=[usuariocreacionid], lazy="raise")
    usuario_ult_mod = relationship(Usuario.__name__, foreign_keys=[usuarioultmodid], lazy="raise")
    tipo_cliente = relationship(TipoCliente.__name__, foreign_keys=[tipoclienteid], lazy="raise")

    # Constructor
    def __init__(self, **kwargs):
        super(Cliente, self).__init__(**kwargs)

    # MÉTODOS
    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'[Cliente] id = {self.id}, codigo = {self.codigo}, nombre = {self.nombre}, apellidos = {self.apellidos}'
