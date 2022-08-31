from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship

from core.dao.modelutils import BaseEntity
from impl.model.usuario import Usuario


class TipoCliente(BaseEntity):
    """Modelo de tipos de cliente."""

    __tablename__ = 'tiposcliente'

    # Mapeo de columnas de la base de datos
    id = Column(Integer, primary_key=True)
    codigo = Column(String, nullable=False)
    descripcion = Column(String, nullable=False)
    fechacreacion = Column(DateTime, nullable=True)
    fechaultmod = Column(DateTime, nullable=True)

    usuariocreacionid = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    usuarioultmodid = Column(Integer, ForeignKey("usuarios.id"), nullable=True)

    # Relaciones con otras tablas
    usuario_creacion = relationship(Usuario.__name__, foreign_keys=[usuariocreacionid], lazy="raise")
    usuario_ult_mod = relationship(Usuario.__name__, foreign_keys=[usuarioultmodid], lazy="raise")

    # Constructor
    def __init__(self, **kwargs):
        super(TipoCliente, self).__init__(**kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'[TipoCliente] tipo_cliente_id = {self.id}, codigo = {self.codigo}, descripcion = {self.descripcion}'
