from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship

from core.dao.modelutils import BaseEntity


class Usuario(BaseEntity):
    """Modelo de usuarios."""

    __tablename__ = 'usuarios'

    # Mapeo de columnas de la base de datos
    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)
    password = Column(Text, nullable=False)

    # Relación n a m de Usuarios-roles
    roles = relationship("Rol", secondary="usuariosroles", back_populates="usuarios", lazy="raise")

    # Constructor
    def __init__(self, **kwargs):
        super(Usuario, self).__init__(**kwargs)

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
        return f'usuario_id = {self.id}, username = {self.username}'
