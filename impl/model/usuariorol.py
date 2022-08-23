from sqlalchemy import Column, Integer, ForeignKey

from core.dao.modelutils import BaseEntity


class UsuarioRol(BaseEntity):
    """Modelo de relación m a n de usuarios-roles."""

    __tablename__ = "usuariosroles"

    rol_id = Column(Integer, ForeignKey("roles.id"), primary_key=True)
    usuario_id = Column(Integer, ForeignKey("usuarios.id"), primary_key=True)

    def __init__(self, **kwargs):
        super(UsuarioRol, self).__init__(**kwargs)

    @classmethod
    def get_id_field_name(cls):
        """
        Devuelve el nombre del campo de la primary key.
        :return: str
        """
        return "rol_id,usuario_id"

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.rol_id == other.rol_id and self.usuario_id == other.usuario_id
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return f'rol_id = {self.rol_id}, usuario_id = {self.usuario_id}'
