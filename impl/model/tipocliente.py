from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from core.dao.basedao import BaseEntity
from impl.model.usuario import Usuario


class TipoCliente(BaseEntity):
    """Modelo de tipos de cliente."""

    __tablename__ = 'tiposcliente'

    # Mapeo de columnas de la base de datos
    id = Column(Integer, primary_key=True)
    codigo = Column(String, nullable=False)
    descripcion = Column(String, nullable=False)

    usuariocreacionid = Column(Integer, ForeignKey("usuarios.id"), nullable=True)
    usuarioultmodid = Column(Integer, ForeignKey("usuarios.id"), nullable=True)

    # Relaciones con otras tablas
    usuario_creacion = relationship(Usuario.__name__, foreign_keys=[usuariocreacionid], lazy="raise")
    usuario_ult_mod = relationship(Usuario.__name__, foreign_keys=[usuarioultmodid], lazy="raise")

    # Constructor
    def __init__(self, **kwargs):
        super(TipoCliente, self).__init__(**kwargs)

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
        return f'tipo_cliente_id = {self.id}, codigo = {self.codigo}, descripcion = {self.descripcion}'
