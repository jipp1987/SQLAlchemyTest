import abc
import enum
from collections import namedtuple
import threading
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Union

from sqlalchemy import create_engine, select, and_, or_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, declarative_base, contains_eager, aliased
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


@dataclass(frozen=True)
class _SQLModelHelper(object):
    """Clase auxiliar para tener mejor identificados los distintos atributos relacionados con los alias de los
    campos que deben utilizarse en la consulta."""
    model_type: type
    model_alias: any
    model_field_value: any
    model_owner_type: any
    field_name: str
    owner_breadcrumb: List[tuple]


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
                                                f'{host}:{port}/{dbname}', pool_size=20, max_overflow=0, echo=True)

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
        # Antes de hacer commit, hago un flush() para pasar cualquier cambio pendiente a la transacción y luego un
        # expunge_all para liberar los objetos dentro de la sesión, para que se puedan utilizar desde fuera.
        cls.get_session_for_current_thread().flush()
        cls.get_session_for_current_thread().expunge_all()
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
        # Cerrar sesión
        cls.get_session_for_current_thread().close()

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

    # SELECT

    def select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None) \
            -> List[BaseEntity]:
        """
        Hace una consulta a la base de datos.
        """
        my_session = type(self).get_session_for_current_thread()

        # Diccionario de alias de campos para utilizar a lo largo de la query. La clave es el nombre del campo tal cual
        # viene en la join_clause
        aliases_dict: Dict[str, _SQLModelHelper] = {}

        # Primero tengo que examinar las cláusulas join para calcular los alias de las distintas
        # tablas involucradas en la query. Esto es importante para consultas en las que se hace join más de una vez
        # sobre una misma tabla.
        if join_clauses:
            self.__resolve_field_aliases(join_clauses=join_clauses, aliases_dict=aliases_dict)

        # Expresión de la consulta
        stmt = select(self.entity_type)

        # Resolver cláusula join
        if join_clauses:
            stmt = self.__resolve_join_clause(join_clauses=join_clauses, stmt=stmt, alias_dict=aliases_dict)

        # Resolver cláusula where
        if filter_clauses:
            stmt = self.__resolve_filter_clauses(filter_clauses=filter_clauses, stmt=stmt)

        stmt = stmt.order_by(self.entity_type.id.desc())

        # Ejecutar la consulta
        result = my_session.execute(stmt).scalars().all()

        # Para evitar problemas, hago flush y libero todos los elementos
        my_session.flush()
        my_session.expunge_all()

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

    @staticmethod
    def __resolve_join_clause(join_clauses: List[JoinClause], stmt, alias_dict: Dict[str, _SQLModelHelper]):
        """
        Resuelve la cláusula join.
        :param join_clauses: Lista de cláusulas join.
        :param alias_dict: Diccionario de alias.
        :returns: Statement SQL con los joins añadidos.
        """
        for j in join_clauses:
            # Recuepero el valor del join, el campo del modelo por el que se va a hacer join
            relationship_to_join = alias_dict[j.relationship_field_name].model_field_value

            # Recupero el alias calculated anteriormente
            alias = alias_dict[j.relationship_field_name].model_alias

            # Compruebo si es una entidad anidada sobre otra entidad a través del campo owner_breadcrumb
            join_options = []
            if alias_dict[j.relationship_field_name].owner_breadcrumb:
                bread_crumbs = alias_dict[j.relationship_field_name].owner_breadcrumb
                for b in bread_crumbs:
                    join_options.append(b[0])

            # Añadir siempre el valor correspondiente al join actual al final, para respetar la "miga de pan"
            join_options.append(relationship_to_join.of_type(alias))

            # OJO!!! Para el caso de relaciones anidadas en otras, hay que hacer tantos joins como corresponda a la
            # "miga de pan" del valor actual del join.
            # Comprobar el tipo de join
            for o in join_options:
                if j.join_type == EnumJoinTypes.LEFT_JOIN:
                    # Importante añadir una opción para forzar que traiga la relación cargada en el objeto
                    stmt = stmt.outerjoin(o)
                else:
                    stmt = stmt.join(o)

            # Si tiene fetch, añadir una opción para traerte todos los campos para rellenar el objeto relation_ship.
            # OJO!!! Importante utilizar "of_type(alias)" para que sea capaz de resolver el alias asignado
            # a cada tabla.
            if j.is_join_with_fetch:
                # Compruebo si es una entidad anidada sobre otra entidad a través del campo owner_breadcrumb
                stmt = stmt.options(contains_eager(*join_options))

        return stmt

    def __resolve_field_aliases(self, join_clauses: List[JoinClause], aliases_dict: dict) -> None:
        """
        Resuelve los alias de las tablas de la consulta.
        :param join_clauses: Lista de cláusulas join.
        :param aliases_dict: Diccionario clave-valor para contener la información.
        :return: None
        """
        # Creo un namedtuple para la operación de ordenación. La idea es separar los campos por el separador "." y
        # ordenarlos en función del tamaño del array resultante, así los campos más anidados estarán al final y el
        # diccionario siempre contendrá a sus "padres" antes de tratarlo.
        join_sorted = namedtuple("join_sorted", ["join_split", "join_clause"])
        join_sorted_list: List[join_sorted] = []

        # Primera pasada para ordenar las join_clauses
        for join_clause in join_clauses:
            rel_split = join_clause.relationship_field_name.split(".")
            join_sorted_list.append(join_sorted(join_split=rel_split, join_clause=join_clause))

        # Ordenar la lista en función del número de elementos
        join_sorted_list = sorted(join_sorted_list, key=lambda x: len(x.join_split), reverse=False)

        for sorted_element in join_sorted_list:
            join_clause = sorted_element.join_clause
            relationship_to_join_class: Union[type, None] = None

            key: str = join_clause.relationship_field_name

            # El campo a comprobar será siempre el último elemento del array split
            field_to_check: str = sorted_element.join_split[-1]
            class_to_check = self.entity_type

            # Primero intento recuperar el valor del mapa, para así obtener los datos del elemento inmediatamente
            # anterior. La clave a recuperar no es la actual, sino la del elemento anterior, para lo cual tengo que
            # acceder al penúltimo nivel del array
            if len(sorted_element.join_split) > 1:
                previous_key: str = ".".join(sorted_element.join_split[:-1])
                class_to_check = aliases_dict[previous_key].model_type

            # Si no es el caso, asumimos que pertenece a la entidad principal del dao
            relationship_to_join_value: any = getattr(class_to_check, field_to_check)

            # Esto lo necesito porque si es una entidad anidad sobre otra entidad anidada, necesito toda
            # la "miga de pan" para que el join funcione correctamente, si sólo especifico el último valor no entenderá
            # de dónde viene la entidad.
            owner_breadcrumb: List[tuple] = []
            if len(sorted_element.join_split) > 1:
                previous_key: str = ".".join(sorted_element.join_split[:-1])
                # Primero añado la lista que ya tuviera el propietario, a modo de miga de pan
                owner_breadcrumb.extend(aliases_dict[previous_key].owner_breadcrumb)
                # Luego añado la que le corresponde a sí mismo, que es la del registro anterior.
                owner_breadcrumb.append((aliases_dict[previous_key].model_field_value,
                                         aliases_dict[previous_key].model_alias))

            # Busco el tipo de entidad para generar un alias. Utilizo el mapa de relaciones de la propia entidad.
            for att in class_to_check.__mapper__.relationships:
                rel_field_name = att.key
                if rel_field_name == field_to_check:
                    relationship_to_join_class = att.mapper.class_
                    break

            # Calculo el alias y lo añado al diccionario, siendo la clave el nombre del campo del join
            alias = aliased(relationship_to_join_class, name="_".join(sorted_element.join_split))
            # Añado un objeto al mapa para tener mejor controlados estos datos
            aliases_dict[key] = _SQLModelHelper(model_type=relationship_to_join_class,
                                                model_alias=alias,
                                                model_owner_type=class_to_check,
                                                owner_breadcrumb=owner_breadcrumb,
                                                field_name=field_to_check,
                                                model_field_value=relationship_to_join_value)
