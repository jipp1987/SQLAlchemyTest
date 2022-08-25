from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from core.dao.modelutils import BaseEntity


class Rol(BaseEntity):
    """Modelo de roles."""

    __tablename__ = 'roles'

    # Mapeo de columnas de la base de datos
    id = Column(Integer, primary_key=True)
    nombre = Column(String(30), nullable=False)

    # Relación n a m de Usuarios-roles. Utilizo noload porque quiero que este objeto sea estrictamente de escritura,
    # lo completo yo a nivel de servicio.
    usuarios_roles = relationship("UsuarioRol", back_populates="rol", lazy="noload")

    # Constructor
    def __init__(self, **kwargs):
        super(Rol, self).__init__(**kwargs)
        # self.usuarios_transient = []

    # Esto lo necesito para que funcione el campo transient
    # @orm.reconstructor
    # def init_on_load(self):
    #     self.usuarios_transient = []

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.id == other.id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'[Rol] id = {self.id}, nombre = {self.nombre}'
