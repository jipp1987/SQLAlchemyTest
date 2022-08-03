import abc
import enum
from collections import namedtuple
import threading
from copy import deepcopy
from typing import Dict, List

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, declarative_base

from core.dao.daotools import FilterClause, EnumFilterTypes

_SQLEngineTypes = namedtuple('SQLEngineTypes', ['value', 'engine_name'])
"""Tupla para propiedades de EnumSQLEngineTypes. La uso para poder añadirle una propiedad al enumerado, aparte del 
propio valor."""


class EnumSQLEngineTypes(enum.Enum):
    """Enumerado de tipos de OrderBy."""

    @property
    def engine_name(self):
        return self.value.engine_name

    MYSQL = _SQLEngineTypes(1, 'mysql+pymysql')
    POSTGRESQL = _SQLEngineTypes(2, 'postgresql')
    SQL_SERVER = _SQLEngineTypes(3, 'pyodbc')
    ORACLE = _SQLEngineTypes(4, 'oracle')
    SQL_LITE = _SQLEngineTypes(5, 'sqlite')


BaseEntity = declarative_base()
"""Declaración de clase para mapeo de todas la entidades de la base de datos."""


class BaseDao(object, metaclass=abc.ABCMeta):
    """Clase abstracta pensada para generar capas de acceso a datos."""

    __db_engine: EnumSQLEngineTypes = None
    """Motor de la base de datos."""

    __db_config: dict = None
    """Diccionario con los datos de conexión con la base de datos. Nulo por defecto."""

    __POOL = None
    """Pool de conexiones para aplicación multihilo, nulo por defecto."""

    __sqlalchemy_engine: Engine = None
    """Engine de sqlalchemy"""

    __session_maker: sessionmaker = None
    """Objeto para fabricar sesiones de sqlalchemy (transacciones)."""

    __thread_session_dict: Dict[int, any] = {}
    """Mapa de sesiones por hilo. El hilo de ejecución es un int."""

    def __init__(self, table: str, entity_type: type(BaseEntity)):
        self.__table = table
        """Nombre de la tabla principal."""
        self.entity_type = entity_type
        """Tipo de entidad."""

    @classmethod
    def set_db_config_values(cls, host: str, username: str, password: str, dbname: str, port: int = 3306,
                             db_engine: EnumSQLEngineTypes = EnumSQLEngineTypes.MYSQL, charset: str = 'utf8'):
        """
        Inicializa la configuración de la base de datos.
        :param host: URL de la base de datos.
        :param port: Puerto; 3306 por defecto.
        :param username: Usuario de la base de datos que va a conectarse.
        :param password: Password de conexión del usuario de la base de datos.
        :param dbname: Nombre de la base de datos.
        :param db_engine: Engine de la base de datos; "MySQL" por defecto.
        :param charset: Set de caracteres de la base de datos, utf8 por defecto.
        :return: None
        """
        # Establecer parámetros de la base de datos.
        cls.__db_config = {
            "host": host,
            "user": username,
            "password": password,
            "db": dbname,
            "port": port,
            'charset': charset
        }

        # Establecer motor de db seleccionado.
        cls.__db_engine = db_engine

        # La cadena de conexión siempre sigue el mismo patrón: dialect+driver://username:password@host:port/database
        # Al crear un engine con la función create_engine(), se genera un pool QueuePool que viene configurado
        # como un pool de 5 conexiones como máximo por defecto; lo cambio para añadir unas cuantas más.
        cls.__sqlalchemy_engine = create_engine(f'{db_engine.engine_name}://{username}:{password}@'
                                                f'{host}:{port}/{dbname}', pool_size=20, max_overflow=0)

        # Inicializar el creador de sesiones (transacciones)
        cls.__session_maker = sessionmaker(bind=cls.__sqlalchemy_engine)

        # Esta línea lo que hace es forzar la creación de las tablas en la base de datos si no existieran. Se basa en
        # las clases que heredad de BaseEntity.
        BaseEntity.metadata.create_all(cls.__sqlalchemy_engine)

    @staticmethod
    def __get_current_thread() -> int:
        """
        Devuelve un identificador del hilo actual de ejecución.
        :return: int
        """
        return threading.get_ident()

    @classmethod
    def is_there_any_session_in_current_thread(cls) -> bool:
        """
        Devuelve true si hay alguna sesión en el hilo actual; devuevle false en caso contrario.
        :return: bool
        """
        return True if cls.__get_current_thread() in cls.__thread_session_dict else False

    @classmethod
    def create_session(cls) -> None:
        """Crea para realizar una transacción en la base de datos y la almacena en el hilo actual."""
        # return scoped_session(type(self).__session_maker)
        # Existe scoped_session que ya me devuelve una sesión almacenada en thread.local, es decir,
        # una sesión por hilo gestionada de forma automática. Sin embargo, en este caso prefiero gestionar yo
        # manualmente las sesiones a través de un mapa de sesión por hilo dentro del dao, para mejor control
        # desde esta clase. Establezco autoflush y autocommit a false, prefiero controlar manualmente los cambios en
        # la transacción / base de datos.
        cls.__thread_session_dict[cls.__get_current_thread()] = \
            cls.__session_maker(autocommit=False, autoflush=False, expire_on_commit=True)

    @classmethod
    def get_session_for_current_thread(cls):
        """
        Devuelve la sesión asociada al hilo de ejecución.
        :return: Sesión del hilo actual.
        """
        # Si no existe sesión para el hilo, lo creo
        return cls.__thread_session_dict[cls.__get_current_thread()]

    @classmethod
    def commit(cls) -> None:
        """
        Hace commit de los cambios en la transacción asociada al hilo de ejecución.
        :return: None
        """
        cls.get_session_for_current_thread().commit()

    @classmethod
    def rollback(cls) -> None:
        """
        Hace rollback (deshace) de los cambios en la transacción asociada al hilo de ejecución.
        :return: None
        """
        cls.get_session_for_current_thread().rollback()

    @classmethod
    def close_session(cls) -> None:
        """
        Cierra la sesión del hilo de ejecución.
        :return: None
        """
        my_session = cls.get_session_for_current_thread()

        # Eliminar todos los objetos de la sesión
        my_session.expunge_all()
        # Cerrar sesión
        my_session.close()

        # Eliminar sesión del mapa
        cls.__thread_session_dict.pop(cls.__get_current_thread())

    def get_entity_id_field_name(self):
        """
        Devuelve el nombre del campo id de la entidad principal asociada al dao.
        :return: str
        """
        id_field_name: str = "id"
        id_field_name_fn = getattr(self.entity_type, "get_id_field_name")

        if id_field_name_fn is not None:
            id_field_name = id_field_name_fn()

        return id_field_name

    # MÉTODOS DE ACCESO A DATOS

    def create(self, registry: BaseEntity) -> None:
        """
        Crea una entidad en la base de datos.
        :param registry:
        :return: None
        """
        my_session = type(self).get_session_for_current_thread()

        # Hago una copia del objeto tal cual está actualmente
        registry_to_create = deepcopy(registry)

        # Ejecutar consulta
        my_session.add(registry_to_create)
        # Importante hacer flush para que se refleje el cambio en la propia transacción (sin llegar a hacer commit
        # en la db)
        my_session.flush()

        # Recuperar el último registro para obtener el id asignado y establecerlo como el id de la entidad pasada como
        # parámetro
        id_field_name = self.get_entity_id_field_name()
        id_field = getattr(registry_to_create, id_field_name)

        # Consultar el último registro para recuperar su id
        setattr(registry, id_field_name, id_field)

    def find_last_entity(self) -> BaseEntity:
        """
        Devuelve el último registro introducido en la tabla principal del DAO.
        :return: BaseEntiity
        """
        my_session = type(self).get_session_for_current_thread()

        # Recuperar el último registro para obtener el id asignado y establecerlo como el id de la entidad pasada como
        # parámetro
        id_field_name = self.get_entity_id_field_name()
        id_field = getattr(self.entity_type, id_field_name)

        return my_session.query(self.entity_type).order_by(id_field.desc()).first()

    def delete_by_id(self, registry_id: any):
        """
        Elimina un registro por id.
        :param registry_id: Id del registro.
        :return: None.
        """
        my_session = type(self).get_session_for_current_thread()

        # Buscar método para obtener el id
        id_field_name: str = self.get_entity_id_field_name()
        id_field = getattr(self.entity_type, id_field_name)

        my_session.query(self.entity_type).filter(id_field == registry_id).delete()

    def find_by_id(self, registry_id: any):
        """
        Devuelve un registro a partir de un id.
        :param registry_id: Id del registro en la base de datos.
        :return: Una instancia de la clase principal del dao si el registro exite; None si no existe.
        """
        my_session = type(self).get_session_for_current_thread()
        result = my_session.query(self.entity_type).get(registry_id)

        return result

    def select(self, filter_clauses: List[FilterClause] = None):
        my_session = type(self).get_session_for_current_thread()

        stmt = select(self.entity_type)

        if filter_clauses:
            for f in filter_clauses:
                # Primero voy recuperando los campos por nombre de la entidad objetivo
                field_to_filter_by = getattr(self.entity_type, f.field_name)
                # Expresión a añadir
                expression: any = None

                # Voy comprobando el tipo de filtro y construyendo la expresión de forma adecuada según los criterios de
                # SQLAlchemy
                if f.filter_type == EnumFilterTypes.EQUALS:
                    expression = field_to_filter_by == f.object_to_compare
                elif f.filter_type == EnumFilterTypes.NOT_EQUALS:
                    expression = field_to_filter_by != f.object_to_compare
                elif f.filter_type == EnumFilterTypes.LIKE or f.filter_type == EnumFilterTypes.NOT_LIKE:
                    # Si no incluye porcentaje, le añado yo uno al principio y al final
                    f.object_to_compare = f'%{f.object_to_compare}%' if "%" not in f.object_to_compare \
                        else f.object_to_compare
                    expression = field_to_filter_by.like(f.object_to_compare) if f.filter_type == EnumFilterTypes.LIKE \
                        else field_to_filter_by.not_like(f.object_to_compare)

                # Voy concatenando los filtros al statement
                stmt = stmt.where(expression)

        stmt = stmt.order_by(self.entity_type.id.desc())
        result = my_session.execute(stmt).scalars().all()

        # Retiro los objetos de la sesión para poder trabajar con ellos desde fuera
        for r in result:
            my_session.expunge(r)

        return result
