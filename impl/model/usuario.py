from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship

from core.dao.modelutils import BaseEntity


class Usuario(BaseEntity):
    """Modelo de usuarios."""

    __tablename__ = 'usuarios'

    # Mapeo de columnas de la base de datos
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    password = Column(Text(60), nullable=False)

    # Relación n a m de Usuarios-roles. Utilizo noload porque quiero que este objeto sea estrictamente de escritura,
    # lo completo yo a nivel de servicio.
    usuarios_roles = relationship("UsuarioRol", lazy="noload")

    # Constructor
    def __init__(self, **kwargs):
        super(Usuario, self).__init__(**kwargs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'[Usuario] id = {self.id}, username = {self.username}'
