import abc
import enum
from collections import namedtuple
import threading
from copy import deepcopy
from typing import Dict, List

from sqlalchemy import create_engine, select, and_, or_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import expression

from core.dao.daotools import FilterClause, EnumFilterTypes, EnumOperatorTypes, JoinClause, EnumJoinTypes

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

    def __resolve_filter_clauses(self, filter_clauses: List[FilterClause], stmt):
        """
        Resuelve el contenido de los filtros.
        :param stmt: Statement al que se le van a añadir los filtros.
        :param filter_clauses:
        :return: Devuelve el statement con los filtros añadidos
        """
        # Contenido final del filtro en forma de listado que se va a añadir al statement
        filter_content = []

        def __inner_resolve_filter_clauses(field_clause: FilterClause):
            """
            Utilizo esta función interna para resolver los filtros anidados en otros filtros (aquéllos que van a ir
            dentro de un mismo paréntesis).
            :param field_clause:
            :return: None
            """
            # Primero voy recuperando los campos por nombre de la entidad objetivo
            field_to_filter_by = getattr(self.entity_type, field_clause.field_name)

            # Calcular operador de unión entre filtros
            f_operator: expression = and_
            if field_clause.operator_type is not None and field_clause.operator_type == EnumOperatorTypes.OR:
                f_operator = or_

            # Paréntesis: si el atributo related_filter_clauses no está vacío, significa que la cláusula está
            # unida a otras dentro de un paréntesis.
            # Creo dos listas, una iniciada con el filtro principal
            inner_filter_list = [field_clause]

            has_related_filters: bool = False

            # Si tiene filtros asociados, los añado a la lista interna de filtros para tratarlos todos como uno sólo,
            # de tal manera que estarán todos dentro de un paréntesis
            if field_clause.related_filter_clauses:
                has_related_filters = True

                for related_filter in field_clause.related_filter_clauses:
                    inner_filter_list.append(related_filter)

            # Lista final de expresiones que se añadirán al operador al final
            inner_expression_list = []

            # Lo normal es que sea un sólo filtro, pero con esto puedo controlar la posibilidad de que haya varios
            # filtros relacionados dentro de un paréntesis.
            for f in inner_filter_list:
                # Expresión a añadir
                filter_expression: any = None

                # Voy comprobando el tipo de filtro y construyendo la expresión de forma adecuada según los criterios de
                # SQLAlchemy
                if f.filter_type == EnumFilterTypes.EQUALS:
                    filter_expression = field_to_filter_by == f.object_to_compare
                elif f.filter_type == EnumFilterTypes.NOT_EQUALS:
                    filter_expression = field_to_filter_by != f.object_to_compare
                elif f.filter_type == EnumFilterTypes.GREATER_THAN:
                    filter_expression = field_to_filter_by > f.object_to_compare
                elif f.filter_type == EnumFilterTypes.LESS_THAN:
                    filter_expression = field_to_filter_by < f.object_to_compare
                elif f.filter_type == EnumFilterTypes.GREATER_THAN_OR_EQUALS:
                    filter_expression = field_to_filter_by >= f.object_to_compare
                elif f.filter_type == EnumFilterTypes.LESS_THAN_OR_EQUALS:
                    filter_expression = field_to_filter_by <= f.object_to_compare
                elif f.filter_type == EnumFilterTypes.LIKE or f.filter_type == EnumFilterTypes.NOT_LIKE:
                    # Si no incluye porcentaje, le añado yo uno al principio y al final
                    f.object_to_compare = f'%{f.object_to_compare}%' if "%" not in f.object_to_compare \
                        else f.object_to_compare
                    filter_expression = field_to_filter_by.like(
                        f.object_to_compare) if f.filter_type == EnumFilterTypes.LIKE \
                        else field_to_filter_by.not_like(f.object_to_compare)
                elif f.filter_type == EnumFilterTypes.IN:
                    filter_expression = field_to_filter_by.in_(f.object_to_compare)
                elif f.filter_type == EnumFilterTypes.NOT_IN:
                    filter_expression = ~field_to_filter_by.in_(f.object_to_compare)
                elif f.filter_type == EnumFilterTypes.STARTS_WITH:
                    f.object_to_compare = f'{f.object_to_compare}%' if not f.object_to_compare.endswith("%") \
                        else f.object_to_compare
                    filter_expression = field_to_filter_by.like(f.object_to_compare)
                elif f.filter_type == EnumFilterTypes.ENDS_WITH:
                    f.object_to_compare = f'%{f.object_to_compare}' if not f.object_to_compare.startswith("%") \
                        else f.object_to_compare
                    filter_expression = field_to_filter_by.like(f.object_to_compare)

                # Voy añadiendo el contenido del filtro a la lista final de expresiones
                inner_expression_list.append(filter_expression)

            # Si tiene filtros asociados, hay que utilizar al final self_group para que los relacione dentro de un
            # mismo paréntesis
            filter_content.append(f_operator(*inner_expression_list).self_group()) if has_related_filters \
                else filter_content.append(f_operator(*inner_expression_list))

        # Hago el proceso para cada filtro del listado, para controlar los filtros anidados en otros (relacionados
        # entre por paréntesis)
        for filter_q in filter_clauses:
            __inner_resolve_filter_clauses(filter_q)

        # Añado el contenido final del filtro a la cláusula where del statement
        return stmt.where(*filter_content)

    def __resolve_join_clause(self, join_clauses: List[JoinClause], stmt):
        """
        Resuelve la cláusula join.
        :param join_clauses: Lista de cláusulas join.
        :returns: Statement SQL con los joins añadidos.
        """
        fields_to_join: list = []

        for j in join_clauses:
            # Comprobar el tipo de join
            if j.join_type == EnumJoinTypes.LEFT_JOIN:
                fields_to_join.append((j.table_name, None, True, False))
            elif j.join_type == EnumJoinTypes.RIGHT_JOIN:
                is_outer = True
                fields_to_join.append((j.table_name, self.entity_type, is_outer))
            else:
                fields_to_join.append(j.table_name)

        return stmt.join(*fields_to_join)

    def select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None) \
            -> List[BaseEntity]:
        """
        Hace una consulta a la base de datos.
        """
        my_session = type(self).get_session_for_current_thread()

        # Expresión de la consulta
        table_list = [self.entity_type]
        if join_clauses:
            for j in join_clauses:
                table_list.append(j.table_name)

        stmt = select(*table_list)

        # Resolver cláusula join
        if join_clauses:
            stmt = self.__resolve_join_clause(join_clauses=join_clauses, stmt=stmt)

        # Resolver cláusula where
        if filter_clauses:
            stmt = self.__resolve_filter_clauses(filter_clauses=filter_clauses, stmt=stmt)

        stmt = stmt.order_by(self.entity_type.id.desc())

        # DEPURACIÓN
        print(f"{str(stmt)}\n")

        # Ejecutar la consulta
        result = my_session.execute(stmt).scalars().all()

        # Retiro los objetos de la sesión para poder trabajar con ellos desde fuera
        def __expunge_select_result(registry):
            """Función para liberar objetos desde sesión. La declaro como función interna para poder liberar los
            objetos de forma recursiva en caso de que tengan anidados otros modelos de datos de forma relacional."""
            # Compruebo si el objeto tiene otras entidades de modelo de datos anidadas
            # Con la siguiente línea obtengo todos los atributos mapeados por SQLAlchemy, incluyendo las relaciones.
            for att in registry.__mapper__.attrs.keys():
                att_value = getattr(registry, att)

                # Si es otra entidad, llamo de forma recursiva a esta función interna para liberar todos los objetos de
                # la DB.
                if att_value is not None and issubclass(type(att_value), BaseEntity):
                    __expunge_select_result(att_value)

            # Importante liberar al objeto principal después de liberar a los asociados, si lo liberase antes todos
            # sus objetos estarían ya caducados en la sesión y se produciría un error.
            my_session.expunge(registry)

        # Liberar de la sesión todos los objetos traídos en la consulta.
        for r in result:
            __expunge_select_result(r)

        return result
