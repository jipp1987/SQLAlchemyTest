import abc
import enum
from collections import namedtuple
import threading
from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Union, Tuple

from sqlalchemy import create_engine, select, and_, or_, inspect, func, insert, delete, Date, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, contains_eager, aliased
from sqlalchemy.sql import expression

from core.dao.daotools import FilterClause, EnumFilterTypes, EnumOperatorTypes, JoinClause, EnumJoinTypes, \
    OrderByClause, GroupByClause, EnumOrderByTypes, FieldClause, EnumAggregateFunctions
from core.dao.modelutils import BaseEntity, find_entity_id_field_name, deserialize_model
from core.utils.dateutils import string_to_datetime_sql

_SQLEngineTypes = namedtuple('SQLEngineTypes', ['value', 'engine_name'])
"""Tupla para propiedades de EnumSQLEngineTypes. La uso para poder añadirle una propiedad al enumerado, aparte del 
propio valor."""

_separator_for_nested_fields: str = "$123$"
"""Separador para establecer un alias interno para los fieldclause, para ser capaz de convertir luego un campo de 
una entidad anidada a la propiedad del objeto correspondiente."""


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
        # las clases que heredan de BaseEntity.
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
        # Si no hay transacción para el hilo actual, lanzar excepción.
        current_thread: int = cls.__get_current_thread()
        if current_thread not in cls.__thread_session_dict:
            raise KeyError("There is not transaction active in the current thread.")

        return cls.__thread_session_dict[current_thread]

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

    def get_entity_id_field_name(self) -> Union[str, List[str]]:
        """
        Devuelve el nombre del campo id de la entidad principal asociada al dao.
        :return: Puede devolver un string con el nombre del campo id, o una lista de strings para entidades con más
        de una primary-key.
        """
        return find_entity_id_field_name(self.entity_type)

    @staticmethod
    def __check_date_fields(registry: BaseEntity) -> None:
        """
        Comprueba los posibles campos fecha de la entidad a persistir para controlar si la fecha ha llegado como string
        y convertirla a date si es necesario.
        :param registry:
        :return: None
        """
        mapper = inspect(type(registry))
        columns = mapper.columns

        field_type: any
        attr: any

        # Recorro las columnas buscando posibles valores de fecha
        for c in columns:
            field_type = c.type
            if isinstance(field_type, Date) or isinstance(field_type, DateTime):
                if hasattr(registry, c.name):
                    # Si es un string, lo convierto a fecha de python.
                    attr = getattr(registry, c.name)
                    if isinstance(attr, str):
                        setattr(registry, c.name, string_to_datetime_sql(attr))

    # MÉTODOS DE ACCESO A DATOS
    def create(self, registry: BaseEntity) -> None:
        """
        Crea una entidad en la base de datos.
        :param registry:
        :return: None
        """
        my_session = type(self).get_session_for_current_thread()

        # En función de si id_field_name es una lista de strings (caso de relaciones n a m) o sólo un string
        # (entidades normales) elaboro el insert de forma diferente.
        id_field_name: Union[List[str], str] = self.get_entity_id_field_name()
        if isinstance(id_field_name, list):
            # Elaboro un diccionario siendo la clave el nombre del campo y el valor el actual del registro respecto
            # esa primary key
            values: dict = {}
            for pk in id_field_name:
                values[pk] = getattr(registry, pk)

            # Statement a ejecutar
            stmt: expression = insert(type(registry)).values(**values)
            my_session.execute(stmt)
            my_session.flush()
        else:
            # Revisar campos fecha
            self.__check_date_fields(registry)

            # Hago una copia del objeto tal cual está actualmente
            registry_to_create = deepcopy(registry)

            # Ejecutar consulta
            my_session.add(registry_to_create)
            # Importante hacer flush para que se refleje el cambio en la propia transacción (sin llegar a hacer commit
            # en la db)
            my_session.flush()

            id_field = getattr(registry_to_create, id_field_name)
            # Establecer el valor del id en el registro pasado como parámetro
            setattr(registry, id_field_name, id_field)

    def update(self, registry: BaseEntity) -> None:
        """
        Modifica una entidad en la base de datos.
        :param registry:
        :return: None
        """
        my_session = type(self).get_session_for_current_thread()

        # Revisar campos fecha
        self.__check_date_fields(registry)

        # Si es una lista, es una entidad con múltiples foreign-keys como una relación n a m
        # filter(entity_class.id_field == entity_to_update.id_value)
        id_field_name: Union[str, List[str]] = self.get_entity_id_field_name()
        filter_for_update: List[expression] = []
        if isinstance(id_field_name, list):
            for pk in id_field_name:
                filter_for_update.append(getattr(self.entity_type, pk) == getattr(registry, pk))
        else:
            filter_for_update.append(getattr(self.entity_type, id_field_name) == getattr(registry, id_field_name))

        # Recorro la lista de atributos del objeto y los almaceno en un diccionario
        mapper = inspect(type(registry))
        values_dict: dict = {}
        for key in mapper.columns:
            values_dict[key.name] = getattr(registry, key.name)

        # Actualizo a través del diccionario
        my_session.query(self.entity_type).filter(*filter_for_update).update(values_dict)

        # Importante hacer flush para que se refleje el cambio en la propia transacción (sin llegar a hacer commit
        # en la db)
        my_session.flush()
        my_session.expunge_all()

    def delete(self, registry: BaseEntity):
        """
        Elimina un registro por id.
        :param registry: Registro a eliminar.
        :return: None.
        """
        my_session = type(self).get_session_for_current_thread()

        # Id de la entidad para determinar la forma de afrontar el delete: si es una pk compuesta como las de las
        # relaciones n a m, o única de tabla normal
        id_field_name: Union[str, List[str]] = self.get_entity_id_field_name()

        if isinstance(id_field_name, list):
            # Construyo una expresión delete where
            stmt: expression = delete(type(registry))
            for pk in id_field_name:
                # where(entity_class.pk_field == pk_value)
                stmt = stmt.where(getattr(type(registry), pk) == getattr(registry, pk))

            my_session.execute(stmt)
        else:
            id_field_value = getattr(registry, id_field_name)
            id_field = getattr(type(registry), id_field_name)
            my_session.query(self.entity_type).filter(id_field == id_field_value).delete()

        my_session.flush()

    def _execute_statement(self, stmt: expression):
        """
        Ejecuta un statement de SQLAlchemy Core.
        :param stmt: Statement de SQLAlchemy Core.
        :return: None
        """
        my_session = type(self).get_session_for_current_thread()
        my_session.execute(stmt)
        my_session.flush()

    def find_by_id(self, registry_id: Union[int, dict], join_clauses: List[JoinClause] = None) \
            -> Union[BaseEntity, None]:
        """
        Devuelve un registro a partir de un id.
        :param registry_id: Id del registro en la base de datos. Puede ser un entero o un diccionario para el caso de
        entidades con múltiples primary-keys como es el caso de las relaciones n a m. Si es un diccionario, la clave
        debe ser el nombre del campo pk y el valor el que se desee consultar.
        :param join_clauses: Cláusulas join.
        :return: Una instancia de la clase principal del dao si el registro exite; None si no existe.
        """
        entity: Union[BaseEntity, None] = None
        filters: List[FilterClause]

        if isinstance(registry_id, dict):
            filters = []
            # En el caso de múltiples pks, creo tantos filterclauses como claves haya
            for key, value in registry_id.items():
                filters.append(FilterClause(field_name=key, filter_type=EnumFilterTypes.EQUALS,
                                            object_to_compare=value))
        else:
            filters = [FilterClause(field_name=self.get_entity_id_field_name(),
                                    filter_type=EnumFilterTypes.EQUALS,
                                    object_to_compare=registry_id)]

        result = self.__select(join_clauses=join_clauses, filter_clauses=filters)

        if result:
            entity = result[0]

        return entity

    def _update_many_to_many(self, many_to_many_old: List[BaseEntity], many_to_many_new: List[BaseEntity]) -> None:
        """
        Compara listas de una relación n a m del mismo tipo de entidad para saber si debe crear nuevos registros
        y/o eliminar registros que ya no están presentes.
        :param many_to_many_old: Lista de entidades many to many anterior, normalmente consultada en la base de datos.
        :param many_to_many_new: Nueva lista de entidades many to many anterior, normalmente consultada en la base de
        datos.
        :return: None
        """
        # Comparo listas para saber qué debo eliminar o crear
        is_exists: bool

        if many_to_many_old:
            for u in many_to_many_old:
                is_exists = False
                for u_new in many_to_many_new:
                    if u == u_new:
                        is_exists = True
                        break

                if not is_exists:
                    self.delete(u)

        for u_new in many_to_many_new:
            is_exists = False
            if many_to_many_old:
                for u in many_to_many_old:
                    if u == u_new:
                        is_exists = True
                        break

            if not is_exists:
                self.create(u_new)

    # SELECT
    def select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None,
               order_by_clauses: List[OrderByClause] = None, limit: int = None, offset: int = None) \
            -> List[BaseEntity]:
        """
        Selecciona entidades cargadas con todos sus campos. Si se incluyem joins con fetch, traerá cargadas también
        las entidades anidadas referenciadas en los joins.
        :param filter_clauses: Cláusula de filtrado.
        :param join_clauses: Clásula de joins.
        :param order_by_clauses: Cláusula de order by.
        :param limit: Límite de resultados.
        :param offset: Índice para paginación de resultados.
        :return: List[BaseEntity]
        """
        return self.__select(filter_clauses=filter_clauses, join_clauses=join_clauses,
                             order_by_clauses=order_by_clauses, limit=limit, offset=offset)

    def select_fields(self, field_clauses: List[FieldClause], filter_clauses: List[FilterClause] = None,
                      join_clauses: List[JoinClause] = None, order_by_clauses: List[OrderByClause] = None,
                      group_by_clauses: List[GroupByClause] = None, limit: int = None, offset: int = None,
                      return_raw_result: bool = False) \
            -> List[dict]:
        """
        Selecciona campos individuales. Los fetch de los joins serán ignorados, sólo se devuelven los campos indicados
        en los field_clauses.
        :param field_clauses: Listado de campos a seleccionar.
        :param filter_clauses: Filtros.
        :param join_clauses: Joins.
        :param order_by_clauses: Order Bys.
        :param group_by_clauses: Group Bys.
        :param limit: Límite de resultados..
        :param offset: Índice para paginación de resultados.
        :param return_raw_result: Si True, devuelve el resultado tal cual, como un listado de
        diccionarios, sin intentar transformarlo a entidad. False por defecto.
        :return: Lista de diccionarios.
        """
        return self.__select(filter_clauses=filter_clauses, join_clauses=join_clauses,
                             order_by_clauses=order_by_clauses, field_clauses=field_clauses,
                             group_by_clauses=group_by_clauses, limit=limit, offset=offset,
                             return_raw_result=return_raw_result)

    def select_by_statement(self, stmt: expression, is_return_row_object: bool) -> List[Union[BaseEntity, Tuple]]:
        """
        Hace una select según una expresión de SQLAlchemy pasada como parámetro.
        :param stmt: Expresión de SQLAlchemy a ejecutar.
        :param is_return_row_object: Si True, devuelve un objeto row (una lista de tuplas); útil para selects de campos
        individuales. Si False, devuelve entidades cargadas completamente.
        :return: List[Union[BaseEntity, Tuple]] En función de cómo se haya confeccionado el statement, devolverá una
        lista de modelos de base de datos o bien una lista de tuplas (normalmente para selects de campos individuales).
        """
        my_session = type(self).get_session_for_current_thread()

        result: List[Union[BaseEntity, Tuple]]
        if is_return_row_object:
            result = my_session.execute(stmt).all()
        else:
            result = my_session.execute(stmt).scalars().all()

        # Para evitar problemas, hago flush y libero todos los elementos
        my_session.flush()
        my_session.expunge_all()

        return result

    def __select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None,
                 order_by_clauses: List[OrderByClause] = None, group_by_clauses: List[GroupByClause] = None,
                 field_clauses: List[FieldClause] = None, limit: int = None, offset: int = None,
                 return_raw_result: bool = False) \
            -> Union[List[BaseEntity], List[tuple]]:
        """
        Hace una consulta a la base de datos.
        """
        my_session = type(self).get_session_for_current_thread()

        # EJEMPLO DE SELECT EN SQLALCHEMY
        # select cliente, cliente.tipocliente, cliente.tipocliente.usuario_creacion, cliente.tipocliente.usuario_ultmod,
        # , cliente.tipocliente.usuario_ultmod, cliente.usuario_ultmod, cliente.usuario_creacion from cliente inner
        # join tipo_cliente left join cliente.usuario_creacion left join cliente.usuario_ult_mod
        # left join cliente.tipo_cliente.usuario_creacion left join cliente.tipo_cliente.usuario_ult_mod
        # where cliente.tipo_cliente.codigo like '%0%' and cliente.tipo_cliente.descripcion like '%a%' or
        # (cliente.tipo_cliente.usuario_creacion.username like '%a%' or
        # cliente.tipo_cliente.usuario_creacion.username like '%e%')
        # where(or_(and_(alias_0.codigo.like("%0%"), alias_0.descripcion.like("%a%")),
        #          or_(alias_4.username.like("%a%"), alias_4.username.like("%e%")).self_group()))

        # SQLALCHEMY
        # alias_0 = aliased(TipoCliente, name="tipo_cliente")
        # alias_1 = aliased(Usuario, name="usuario_creacion")
        # alias_2 = aliased(Usuario, name="usuario_ult_mod")

        # alias_3 = aliased(Usuario, name="tipo_cliente_usuario_ult_mod")
        # alias_4 = aliased(Usuario, name="tipo_cliente_usuario_creacion")

        # stmt = select(Cliente). \
        #     outerjoin(Cliente.usuario_ult_mod.of_type(alias_2)). \
        #     outerjoin(Cliente.usuario_creacion.of_type(alias_1)). \
        #     join(Cliente.tipo_cliente.of_type(alias_0)). \
        #     outerjoin(TipoCliente.usuario_creacion.of_type(alias_4)). \
        #     outerjoin(TipoCliente.usuario_ult_mod.of_type(alias_3)). \
        #     options(
        #     contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_ult_mod.of_type(alias_3)),
        #     contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_creacion.of_type(alias_4)),
        #     contains_eager(Cliente.tipo_cliente.of_type(alias_0)),
        #     contains_eager(Cliente.usuario_creacion.of_type(alias_1)),
        #     contains_eager(Cliente.usuario_ult_mod.of_type(alias_2)),
        # ).where(or_(and_(alias_0.codigo.like("%0%"), alias_0.descripcion.like("%a%")),
        #             or_(alias_4.username.like("%a%"), alias_4.username.like("%e%")).self_group()))

        # PASOS PARA RESOLVER LOS JOINS:
        # 1. Si la select tiene joins, hay que establecer unos alias para todas las tablas involucradas en la consulta.
        # 2. La cláusula SELECT, salvo que sea para selección de campos individuales, selecciona siempre a la tabla
        # principal del dao.
        # 3. Los joins se resuelven de dos partes: por un lado la función join u outerjoin, según sea inner o left, la
        # cual utiliza el campo de la relación mapeada en el modelo y le añade "of_type(alias_x)", y luego una opción
        # contains_eager para forzar a que la entidad venga cargada en el objeto resultado. Importante que, si es una
        # relación de otra relación, añadir la "miga de pan" de los campos de los que proviene la relación; es decir, si
        # hago un join desde Cliente (tabla principal) a tipo_cliente, y luego a usuario_creación (que es un campo de
        # tipo_cliente), la opción contains_eager tendría como parámetros:
        # contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_creacion.of_type(alias_4))
        # 4. Los alias deben utilizarse para el resto de cláusulas, filter, order, group... siempre con el mismo
        # formato ...of_type(alias_x)

        # Diccionario de alias de campos para utilizar a lo largo de la query. La clave es el nombre del campo tal cual
        # viene en la join_clause
        aliases_dict: Dict[str, _SQLModelHelper] = {}

        # Primero tengo que examinar las cláusulas join para calcular los alias de las distintas
        # tablas involucradas en la query. Esto es importante para consultas en las que se hace join más de una vez
        # sobre una misma tabla.
        if join_clauses:
            # Esta función devuelve la lista de joins ordenada de acuerdo a su nivel de "anidación" de entidades,
            # para respetar un orden lógico de joins y evitar resultados duplicados y equívocos en la consulta.
            join_clauses = self.__resolve_field_aliases(join_clauses=join_clauses, alias_dict=aliases_dict)

        # Si hay field_clauses, es una consulta de campos individuales
        is_select_with_fields: bool = False
        # Este diccionario lo voy a necesitar para, una vez he obtenido el resultado de la select por campos,
        # ser capaz de volcar el resultado a cada campo del objeto que le corresponda.
        field_alias_for_result: Dict[str, str] = {}

        if field_clauses:
            fields_to_select = self.__resolve_field_clauses(field_clauses=field_clauses, alias_dict=aliases_dict,
                                                            field_alias_for_result=field_alias_for_result)
            is_select_with_fields = True
            stmt = select(*fields_to_select)
        else:
            # Expresión de la consulta: si no hay field_clauses, es una consulta de carga total de la entidad;
            # si los hay es una consulta de campos individuales.
            stmt = select(self.entity_type)

        # Resolver cláusula join
        if join_clauses:
            stmt = self.__resolve_join_clause(join_clauses=join_clauses, stmt=stmt, alias_dict=aliases_dict,
                                              is_select_with_fields=is_select_with_fields)

        # Resolver cláusula where
        if filter_clauses:
            stmt = self.__resolve_filter_clauses(filter_clauses=filter_clauses, stmt=stmt, alias_dict=aliases_dict)

        # Resolver cláusula group by
        if group_by_clauses:
            stmt = self.__resolve_group_by_clauses(group_by_clauses=group_by_clauses, stmt=stmt,
                                                   alias_dict=aliases_dict)

        # Resolver cláusula order by
        if order_by_clauses:
            stmt = self.__resolve_order_by_clauses(order_by_clauses=order_by_clauses, stmt=stmt,
                                                   alias_dict=aliases_dict)

        # Limit y offset
        if limit is not None:
            stmt = stmt.limit(limit)
            if offset is not None:
                stmt = stmt.offset(offset)

        # Ejecutar la consulta: si es una consulta de campos, devolver una lista de tuplas; si es una consulta
        # total, devolver una lista de objetos BaseEntity, la que corresponda al dao.
        if is_select_with_fields:
            row_result = my_session.execute(stmt).all()
            # Esto devuelve un objeto Row de SQLAlchemy, lo convierto a diccionario
            result = []
            if row_result:
                if return_raw_result:
                    for r in row_result:
                        result.append(dict(r))
                else:
                    result = self.__convert_from_dict_to_entity(row_result, field_alias_for_result)
        else:
            result = my_session.execute(stmt).scalars().all()

        # Para evitar problemas, hago flush y libero todos los elementos
        my_session.flush()
        my_session.expunge_all()

        return result

    def __convert_from_dict_to_entity(self, lst_obj_dict: List[dict], field_alias_for_result: Dict[str, str]) -> \
            List[BaseEntity]:
        """
        Convierte un listado de filas (diccionarios) en en un listado de diccioarios que se corresponde con el modelo
        de las entidades persistidas, anidando diccionarios cuando corresponde, de tal manera que se facilita su
        conversión posterior a entidades como tal.
        :param lst_obj_dict: Listado de diccionarios obtenidos de una consulta por campos de SQLAlchemy.
        :param field_alias_for_result: Alias de campos calculados previamente.
        :return: Listado de diccionarios que se corresponden con el modelo de datos de las entidades.
        """
        # La primera parte del proceso es parecida a resolve_field_aliases: voy a recorrer la lista de resultados y
        # generar un nuevo diccionario para cada uno de ellos con la idea de tener las claves ordenadas por nivel de
        # anidamiento; así, a medida que vaya generando los objetos anidados me aseguro de tener el objeto de nivel más
        # superior siempre creado.
        lst_fields: List[str] = []
        for key in field_alias_for_result.keys():
            lst_fields.append(key)

        field_sorted = namedtuple("field_sorted", ["field_split", "field_name"])
        fields_sorted_list: List[field_sorted] = []
        # Voy a generar un nuevo result_set de tal manera que los valores del diccionario estén ordenados de acuerdo
        # con el tamaño del string que voy a calcular ahora. El tamaño del string no es en sí su longitud sino la
        # cantidad de entidades anidadas que lo conforman (entidad_1.entidad_11.entidad_12...)
        rows_with_fields_sortened: List[dict] = []

        rel_split: list
        for f_name in lst_fields:
            rel_split = f_name.split(_separator_for_nested_fields)
            fields_sorted_list.append(field_sorted(field_split=rel_split, field_name=f_name))

        fields_sorted_list = sorted(fields_sorted_list, key=lambda x: (len(x.field_split), x.field_name),
                                    reverse=False)

        key_: str
        new_dict: dict
        for row in lst_obj_dict:
            new_dict = {}
            for f in fields_sorted_list:
                # Sustituyo el token que utilicé en la consulta por el punto
                key_ = f.field_name.replace(_separator_for_nested_fields, ".")
                # Añado la clave al nuevo diccionario, dado que estoy utilizando la lista de campos ordenada me aseguro
                # de que para campos anidados siempre exista el nivel superior antes de llegar a los inferiores.
                new_dict[key_] = row[f.field_name]

            rows_with_fields_sortened.append(new_dict)

        # Ya tengo un nuevo resultado con cada fila con los campos ordenados, ahora lo que tengo que hace es, para
        # aquellos campos que sean entidades anidadas, ir generando un diccionario dentro del diccionario con los
        # campos que le correspondan a ese nivel de anidamiento
        final_result: List[BaseEntity] = []
        final_dict: dict
        last_dict: dict
        for r in rows_with_fields_sortened:
            final_dict = {}

            for k, v in r.items():
                rel_split = k.split(".")

                # Si divido la clave por el punto y tiene más de un elemento, significa que es una entidad anidada y
                # tengo que ir anidando diccionarios hasta la última posición que será el valor final
                if len(rel_split) > 1:
                    # Inicializo el último diccionario en el diccionario principal
                    last_dict = final_dict

                    for idx, x in enumerate(rel_split):
                        # Divido la operación entre las posiciones previas a la última, que consisten en ir
                        # anidando diccionarios, y la última que almacena el valor como tal
                        if idx < len(rel_split) - 1:
                            # Si no existe la últia clave en el anterior diccionario, inicializo un nuevo diccionario
                            # en ella. Dado que ordené previamente todos las claves de cada diccionario según el nivel
                            # de anidamiento, puedo confiar en que el proceso va a almacenar siempre el valor
                            # donde corresponde.
                            if x not in last_dict:
                                last_dict[x] = {}
                            # Almaceno el último diccionario
                            last_dict = last_dict[x]
                        else:
                            # Al llegar a la última posición, es el valor final del diccionario anidado
                            last_dict[x] = v

                else:
                    # Si la clave sólo tiene una posición, significa que no es un valor anidado y por tanto le puedo
                    # establecer directamente el valor correspondiente
                    final_dict[k] = v

            # Al final guardo un modelo de datos válido
            final_result.append(deserialize_model(final_dict, self.entity_type))

        return final_result

    def __resolve_group_by_clauses(self, group_by_clauses: List[GroupByClause], stmt,
                                   alias_dict: Dict[str, _SQLModelHelper]):
        """
        Resuelve las cláusulas group by.
        :param alias_dict: Diccionario de alias de campos.
        :param group_by_clauses: Lista de cláusulas group by.
        :param stmt: Statement de SQLAlchemy.
        :return: Statement de SQLAlchemy con los order by añadidos.
        """
        # Obtengo la información de los campos
        field_info_dict = self.__resolve_fields_info(aliases_dict=alias_dict, clauses=group_by_clauses)

        for o in group_by_clauses:
            # Recupero la información del campo del diccionario
            field_info = field_info_dict[o.field_name]

            # Información del campo
            field_to_group_by = field_info.field_to_work_with

            stmt = stmt.group_by(field_to_group_by)

        return stmt

    def __resolve_order_by_clauses(self, order_by_clauses: List[OrderByClause], stmt,
                                   alias_dict: Dict[str, _SQLModelHelper]):
        """
        Resuelve las cláusulas order by.
        :param alias_dict: Diccionario de alias de campos.
        :param order_by_clauses: Lista de cláusulas order by.
        :param stmt: Statement de SQLAlchemy.
        :return: Statement de SQLAlchemy con los order by añadidos.
        """
        # Obtengo la información de los campos
        field_info_dict = self.__resolve_fields_info(aliases_dict=alias_dict, clauses=order_by_clauses)

        for o in order_by_clauses:
            # Recupero la información del campo del diccionario
            field_info = field_info_dict[o.field_name]

            # Información del campo
            field_to_order_by = field_info.field_to_work_with

            # Comprobar tipo de order by
            if o.order_by_type == EnumOrderByTypes.DESC:
                stmt = stmt.order_by(field_to_order_by.desc())
            else:
                stmt = stmt.order_by(field_to_order_by.asc())

        return stmt

    def __resolve_field_clauses(self, field_clauses: List[FieldClause], alias_dict: Dict[str, _SQLModelHelper],
                                field_alias_for_result: Dict[str, str]) \
            -> list:
        """
        Resuelve las cláusulas order by.
        :param alias_dict: Diccionario de alias de campos.
        :param field_clauses: Lista de campos a seleccionar.
        :param field_alias_for_result: Diccionario para almacenar el alias asignado para cada campo.
        :return: List[expression]
        """
        # Obtengo la información de los campos
        field_info_dict = self.__resolve_fields_info(aliases_dict=alias_dict, clauses=field_clauses)

        fields_to_select: list = []

        for f in field_clauses:
            # Recupero la información del campo del diccionario
            field_info = field_info_dict[f.field_name]

            # Información del campo
            field_to_select = field_info.field_to_work_with

            # Comprobar si es select distinct.
            if f.is_select_distinct:
                field_to_select = func.distinct(field_to_select)

            # Comprobar si hay función de agregado
            if f.aggregate_function is not None:
                if f.aggregate_function == EnumAggregateFunctions.COUNT:
                    field_to_select = func.count(field_to_select)
                elif f.aggregate_function == EnumAggregateFunctions.MAX:
                    field_to_select = func.max(field_to_select)
                elif f.aggregate_function == EnumAggregateFunctions.MIN:
                    field_to_select = func.min(field_to_select)
                elif f.aggregate_function == EnumAggregateFunctions.SUM:
                    field_to_select = func.sum(field_to_select)
                elif f.aggregate_function == EnumAggregateFunctions.AVG:
                    field_to_select = func.avg(field_to_select)

            # Label o alias del campo
            field_alias_for_query: str = f.field_name

            if f.field_label:
                field_alias_for_query = f.field_label
            else:
                # Si no tiene label, le añado uno siempre: si no es entidad anidada es el nombre mismo del campo y
                # si lo es sustituyo los puntos por un token admitido por SQL
                if "." in f.field_name:
                    field_alias_for_query = f.field_name.replace(".", _separator_for_nested_fields)

            field_to_select = field_to_select.label(field_alias_for_query)
            fields_to_select.append(field_to_select)

            # Añado al diccionario de alias para el resultado la clave campo-alias
            field_alias_for_result[field_alias_for_query] = f.field_name

        return fields_to_select

    def __resolve_filter_clauses(self, filter_clauses: List[FilterClause], stmt,
                                 alias_dict: Dict[str, _SQLModelHelper]):
        """
        Resuelve el contenido de los filtros.
        :param stmt: Statement al que se le van a añadir los filtros.
        :param filter_clauses: Filtro a comprobar
        :param alias_dict: Diccionario de alias para recuperar el alias que le corresponde a la entidad propietaria
        de cada filtro.
        :return: Devuelve el statement con los filtros añadidos
        """

        def __append_all_filters(filter_clause: FilterClause, filter_list: List[FilterClause]):
            filter_list.append(filter_clause)

            if filter_clause.related_filter_clauses:
                for related_filter in filter_clause.related_filter_clauses:
                    __append_all_filters(related_filter, filter_list)

        def __resolve_filter_expression(filter_clause: FilterClause, field_to_filter_by: any, field_type: any) \
                -> expression:
            """
            Resuelve la expresión del filter clause.
            :param filter_clause: Claúsula de filtrado.
            :param field_to_filter_by: Campo de la entidad de referencia para crear la expresión.
            :param field_type: Tipo de campo objetivo del filtro en la entidad de referencia
            :return: expression
            """
            filter_expression: any

            # Tratar el tipo de campo para ciertos casos
            if field_type is not None:
                # Caso para campos de tipo fecha: si llega como string, convertirla a fecha
                if (isinstance(field_type, Date) or isinstance(field_type, DateTime)) \
                        and isinstance(filter_clause.object_to_compare, str):
                    filter_clause.object_to_compare = string_to_datetime_sql(filter_clause.object_to_compare)

            # Voy comprobando el tipo de filtro y construyendo la expresión de forma adecuada según los criterios de
            # SQLAlchemy
            if filter_clause.filter_type == EnumFilterTypes.EQUALS:
                filter_expression = field_to_filter_by == filter_clause.object_to_compare
            elif filter_clause.filter_type == EnumFilterTypes.NOT_EQUALS:
                filter_expression = field_to_filter_by != filter_clause.object_to_compare
            elif filter_clause.filter_type == EnumFilterTypes.GREATER_THAN:
                filter_expression = field_to_filter_by > filter_clause.object_to_compare
            elif filter_clause.filter_type == EnumFilterTypes.LESS_THAN:
                filter_expression = field_to_filter_by < filter_clause.object_to_compare
            elif filter_clause.filter_type == EnumFilterTypes.GREATER_THAN_OR_EQUALS:
                filter_expression = field_to_filter_by >= filter_clause.object_to_compare
            elif filter_clause.filter_type == EnumFilterTypes.LESS_THAN_OR_EQUALS:
                filter_expression = field_to_filter_by <= filter_clause.object_to_compare
            elif filter_clause.filter_type == EnumFilterTypes.LIKE or \
                    filter_clause.filter_type == EnumFilterTypes.NOT_LIKE:
                # Si no incluye porcentaje, le añado yo uno al principio y al final
                filter_clause.object_to_compare = f'%{filter_clause.object_to_compare}%' if "%" not in filter_clause. \
                    object_to_compare else filter_clause.object_to_compare
                filter_expression = field_to_filter_by.like(
                    filter_clause.object_to_compare) if filter_clause.filter_type == EnumFilterTypes.LIKE \
                    else field_to_filter_by.not_like(filter_clause.object_to_compare)
            elif filter_clause.filter_type == EnumFilterTypes.IN:
                filter_expression = field_to_filter_by.in_(filter_clause.object_to_compare)
            elif filter_clause.filter_type == EnumFilterTypes.NOT_IN:
                filter_expression = ~field_to_filter_by.in_(filter_clause.object_to_compare)
            elif filter_clause.filter_type == EnumFilterTypes.STARTS_WITH:
                filter_clause.object_to_compare = f'{filter_clause.object_to_compare}%' if not filter_clause. \
                    object_to_compare.endswith("%") \
                    else filter_clause.object_to_compare
                filter_expression = field_to_filter_by.like(filter_clause.object_to_compare)
            elif filter_clause.filter_type == EnumFilterTypes.ENDS_WITH:
                filter_clause.object_to_compare = f'%{filter_clause.object_to_compare}' if not filter_clause. \
                    object_to_compare.startswith("%") else filter_clause.object_to_compare
                filter_expression = field_to_filter_by.like(filter_clause.object_to_compare)
            else:
                raise ValueError("Filter not supported or not defined.")

            return filter_expression

        def __inner_resolve_filter_clauses(inner_filter_clauses: List[FilterClause], field_info_dict_inner: dict):
            """
            Resuelve la cláusula de filtrado.
            :param inner_filter_clauses:
            :param field_info_dict_inner:
            :return: list
            """
            # Este filtro: f1 and f2 or (f3 or f4). La forma de expresarlo en SQLAlchemy sería:
            # or_(and_(f1, f2), or_(f3, f4).self_group()). Supongamos que fx es ya una expresión ya resuelta de filtros,
            # como == o like. Hay que ir anidando los filtros en el momento en que cambia el operador, teniendo en
            # cuenta que si hay paréntesis ese filtro no envuelve a los otros sino que va por su cuenta con la función
            # "self_group".

            # Para automatizar esto, tengo que recorrer la lista de filtros, y en el momento en que el siguiente
            # elemento cambie de operador, envolver los filtros hasta ese momento en un and_ o un or_, y dejarlo listo
            # para añadirlo en el siguiente filtro tratado (siempre antes de éste). Si el filtro tiene una lista de
            # filtros asociada significa que van juntos dentro de un paréntesis, con lo cual la función deberá
            # llamarse de forma recursiva para resolver estos casos e ir añadiendo el resultado al filtro global.
            filter_expression: expression
            field_info: any
            field_type: type
            field_to_filter_by: any
            expression_for_nested_filter: expression

            aux_expression_list: List[expression] = []
            # Inicializo el operador a None: la clave del proceso es comprobar los cambios de operador entre filtros
            f_operator: Union[expression, None] = None
            f_operator_nested: expression

            # Lista global de filtros computados y concatenados por los correspondientes operadores
            global_filter_content: Union[None, expression] = None

            for idx, f in enumerate(inner_filter_clauses):
                # Si el operador es None, significa que el elemento actual tiene un operador diferente que el anterior y
                # por tanto hay que encadenar el filtro al actual.
                if f_operator is None:
                    f_operator = or_ if f.operator_type == EnumOperatorTypes.OR else and_

                # Recupero la información del campo del diccionario
                field_info = field_info_dict_inner[f.field_name]

                # Información del campo

                # Tratar este campo en el futuro, principalmente para filtros por fechas
                field_type: any = field_info.field_type

                field_to_filter_by = field_info.field_to_work_with

                # Expresión a añadir
                filter_expression = __resolve_filter_expression(filter_clause=f, field_to_filter_by=field_to_filter_by,
                                                                field_type=field_type)

                # Comprobar si tiene filtros anidados: si los tiene, llamar de forma recursiva a esta función para
                # resolverlos (incluyendo si esos filtros anidados tienen a su vez otros filtros anidados)
                if f.related_filter_clauses:
                    # Estoy envolviendo el contenido en el operador del filtro propietario de los filtros anidados,
                    # primero lo pongo a él y luego la resolución de los filtros asociados (que a su vez pueden
                    # contener otros filtros, pero al llamar de forma recursiva a la función se resolverán todos)

                    # OJO!!! El operador que engloba este filtro interno es el del primer filtro asociado, sino cogerá
                    # siempre el del filtro "padre" y la consulta no será correcta.
                    f_operator_nested = or_ if f.related_filter_clauses[0].operator_type == EnumOperatorTypes.OR \
                        else and_

                    expression_for_nested_filter = f_operator_nested(filter_expression,
                                                                     __inner_resolve_filter_clauses(
                                                                         f.related_filter_clauses,
                                                                         field_info_dict_inner)).self_group()
                    aux_expression_list.append(expression_for_nested_filter)
                else:
                    # Añadirla a la lista auxiliar que va reiniciándose con cada cambio de operador entre filtros
                    aux_expression_list.append(filter_expression)

                # Comprobar el operador del siguiente elemento del listado para ver si ha cambiado: si cambia, hay que
                # agrupar el filtro en el filtro global
                if idx < len(inner_filter_clauses) - 1 and inner_filter_clauses[idx + 1].operator_type != \
                        f.operator_type or idx == len(inner_filter_clauses) - 1:
                    global_filter_content = f_operator(*aux_expression_list) if global_filter_content is None \
                        else f_operator(global_filter_content, *aux_expression_list)

                    # Reinicio del operador para la siguiente iteración
                    f_operator = None
                    aux_expression_list = []
                else:
                    # En este caso que no haga nada, que continúe con el bucle porque significa que el siguiente filtro
                    # está unido a éste con el mismo operador y deben resolverse los dos a la vez.
                    pass

            return global_filter_content

        # Calculo los valores de los campos para reutilizarlos en la función interna de resolución y así optimizar
        # el proceso. Hay que considerar también los posibles filtros internos
        all_filter_clauses: List[FilterClause] = []
        for filter_q in filter_clauses:
            __append_all_filters(filter_q, all_filter_clauses)

        # Obtengo la información del campo
        field_info_dict = self.__resolve_fields_info(aliases_dict=alias_dict, clauses=all_filter_clauses)

        # Hago el proceso para cada filtro del listado, para controlar los filtros anidados en otros (relacionados
        # entre por paréntesis)
        filter_content = __inner_resolve_filter_clauses(filter_clauses, field_info_dict)

        # Añado el contenido final del filtro a la cláusula where del statement
        return stmt.where(filter_content)

    @staticmethod
    def __resolve_join_clause(join_clauses: List[JoinClause], stmt, alias_dict: Dict[str, _SQLModelHelper],
                              is_select_with_fields: bool = False):
        """
        Resuelve la cláusula join.
        :param join_clauses: Lista de cláusulas join.
        :param alias_dict: Diccionario de alias.
        :param is_select_with_fields: Si True, significa que es una selección de campos individuales y por tanto se
        ignorará la opción "fetch" (traer toda la entidad y cargarla sobre la relación del modelo) de los joins.
        :returns: Statement SQL con los joins añadidos.
        """
        join_options_final: list = []
        """Lista de opciones para el join, para añadirlo al final"""

        # Declaración de campos a emplear en el bucle
        join_options: list
        """Lista de opciones para cada claúsula join."""
        relationship_to_join: any
        """Campo de relación a unir."""
        alias: str
        """Alias de la tabla. La forma de representarlo en la consulta es: 
        join(Cliente.tipo_cliente.of_type(alias_0))"""
        is_outer: bool
        """Bool para saber si es un left_join o un inner_join."""
        bread_crumbs: list
        """Lista de migas de pan de la entidad desde la principal del DAO hasta la objetivo del join. Por ejemplo: 
        "tipo_cliente.usuario_creacion" sería: Cliente.tipo_cliente, TipoCliente.usuario_ult_mod.of_type(alias_X). 
        De alguna manera el ORM debe saber de dónde viene el campo."""

        for j in join_clauses:
            # Right join no tiene implementación como tal en SQLAlchemy, hay que crear un statement especial para
            # simularlo y eso no lo puedo contemplar en el select genérico.
            if j.join_type is not None and j.join_type == EnumJoinTypes.RIGHT_JOIN:
                raise ValueError("RIGHT JOIN is not supported for generic BaseDao SELECTs. In order to perform "
                                 "a query with RIGHT JOIN, please create a custom SQLAlchemy statement and use it "
                                 "on \"select_by_statement\" method.")

            # Recupero el valor del join, el campo del modelo por el que se va a hacer join
            relationship_to_join = alias_dict[j.field_name].model_field_value

            # Recupero el alias calculado anteriormente
            alias = alias_dict[j.field_name].model_alias

            # Compruebo si es una entidad anidada sobre otra entidad a través del campo owner_breadcrumb
            join_options = []
            if alias_dict[j.field_name].owner_breadcrumb:
                bread_crumbs = alias_dict[j.field_name].owner_breadcrumb
                for b in bread_crumbs:
                    join_options.append(b[0])

            # Añadir siempre el valor correspondiente al join actual al final, para respetar la "miga de pan"
            # OJO!!! Importante utilizar "of_type(alias)" para que sea capaz de resolver el alias asignado
            # a cada tabla.
            join_options.append(relationship_to_join.of_type(alias))

            # Comprobar el tipo de join; utilizo sólo el último elemento de join_options, en la función del join no
            # hace falta cargar toda la miga de pan, sólo el elemento hacia el que se hace join.
            is_outer = True if j.join_type is not None and j.join_type == EnumJoinTypes.LEFT_JOIN else False
            stmt = stmt.join(join_options[-1], isouter=is_outer)

            # Si tiene fetch, añadir una opción para traerte todos los campos para rellenar el objeto relation_ship.
            if j.is_join_with_fetch and not is_select_with_fields:
                # Para aquéllas entidades anidadas en otras, aquí hay que cargar toda la miga de pan para que el motor
                # sepa resolver la relación entre objetos.
                join_options_final.append(contains_eager(*join_options))

        # Añadir las opciones al final
        if join_options_final:
            stmt = stmt.options(*join_options_final)

        return stmt

    def __resolve_field_aliases(self, join_clauses: List[JoinClause], alias_dict: dict) -> List[JoinClause]:
        """
        Resuelve los alias de las tablas de la consulta.
        :param join_clauses: Lista de cláusulas join.
        :param alias_dict: Diccionario clave-valor para contener la información.
        :return: Devuelve una nueva lista de joins ordenadas por nivel de anidamiento, es decir, las entidades más
        anidadas contando desde la entidad principal se situarán en las últimas posiciones. Es importante respetar este
        orden para que la consulta funcione bien.
        """
        join_sorted = namedtuple("join_sorted", ["join_split", "join_clause"])
        """Creo un namedtuple para la operación de ordenación. La idea es separar los campos por el separador "." y
        ordenarlos en función del tamaño del array resultante, así los campos más anidados estarán al final y el
        diccionario siempre contendrá a sus "padres" antes de tratarlo."""
        join_sorted_list: List[join_sorted] = []
        """Lista de objetos auxiliares namedtuple para ordenar los elementos de la lista de joins según su nivel de 
        anidamiento."""

        # Declaración de campos a emplear en el bucle
        rel_split: list
        """Se utiliza para la ordenación de los joins teniendo en cuenta su nivel de anidamiento."""
        join_clauses_sortened: List[JoinClause]
        """Lista de joins ordenada, con independecia de lo enviado por el llamante del método select."""
        join_clause: JoinClause
        """Join clause a tratar."""
        relationship_to_join_class: Union[type, None]
        """Clase del campo relación correspondiente al join."""
        key: str
        """Clave a almacenar en el diccionario."""
        field_to_check: str
        """Nombre del campo a comprobar"""
        class_to_check: any
        """Clase a comprobar."""
        key_for_breadcrumb: str
        """Clave para la construcción de la miga de pan para aquéllos joins cuyo campo está anidado en otro, 
        por ejemplo tipo_cliente.usuario_creacion."""
        relationship_to_join_value: any
        """Atributo del campo de la relación asociada al join."""
        owner_breadcrumb: List[tuple]
        """Miga de pan del elemento inmediatamente anterior al perteneciente al join, que debe añadirse siempre a 
        la miga de pan propia inmediatamente antes de su propio campo."""
        rel_field_name: str
        """Nombre del campo de la relación."""

        # Primera pasada para ordenar las join_clauses
        for join_clause in join_clauses:
            rel_split = join_clause.field_name.split(".")
            join_sorted_list.append(join_sorted(join_split=rel_split, join_clause=join_clause))

        # Ordenar la lista en función del número de elementos como primer criterio y por el nombre del campo como
        # segundo criterio: los elementos con el mismo tamaño irán juntos, y al ordenarlos alfabéticamente irán juntos
        # también los que tengan la misma entidad origen (por ejemplo, tipo_cliente.usuario_creacion y
        # tipo_cliente.usuario_ult_mod)
        join_sorted_list = sorted(join_sorted_list, key=lambda x: (len(x.join_split), x.join_clause.field_name),
                                  reverse=False)

        # Es importante que los joins estén ordenados en la consulta final, aprovecho la lista auxiliar
        # ordenada para rehacer la lista original
        join_clauses_sortened = []

        for sorted_element in join_sorted_list:
            join_clause = sorted_element.join_clause
            join_clauses_sortened.append(join_clause)
            relationship_to_join_class = None

            key = join_clause.field_name

            # El campo a comprobar será siempre el último elemento del array split
            field_to_check = sorted_element.join_split[-1]
            # En principio asumo que la clase origen será la principal, aunque si al separar el nombre del campo del
            # join por el punto "." hay varios elementos, significa que es un join anidado en otro join.
            class_to_check = self.entity_type

            # Esto lo necesito porque si es una entidad anidad sobre otra entidad anidada, necesito toda
            # la "miga de pan" para que el join funcione correctamente, si sólo especifico el último valor no entenderá
            # de dónde viene la entidad. Es decir, es aspecto que tiene la sentencia para SQLAlchemy es éste:
            # contains_eager(Cliente.tipo_cliente, TipoCliente.usuario_ult_mod.of_type(alias_3)).
            owner_breadcrumb = []

            # Primero intento recuperar el valor del mapa, para así obtener los datos del elemento inmediatamente
            # anterior. La clave a recuperar no es la actual, sino la de algún elemento anterior, para lo cual tengo que
            # acceder al penúltimo nivel del array. Como están ordenados por tamaño y alfabéticamente, el elemento
            # origen siempre va a existir en el mapa en el momento de procesar un join anidado en otro campo.
            if len(sorted_element.join_split) > 1:
                key_for_breadcrumb = ".".join(sorted_element.join_split[:-1])
                # La clase anidada ya habrá sido procesada anteriormente debido al orden de los elementos,
                # con lo cual esto siempre encontrará el objeto.
                class_to_check = alias_dict[key_for_breadcrumb].model_type
                # Primero añado la lista que ya tuviera el propietario, a modo de miga de pan
                owner_breadcrumb.extend(alias_dict[key_for_breadcrumb].owner_breadcrumb)
                # Luego añado la que le corresponde a sí mismo, que es la del registro anterior.
                owner_breadcrumb.append((alias_dict[key_for_breadcrumb].model_field_value,
                                         alias_dict[key_for_breadcrumb].model_alias))

            # Si no es el caso, asumimos que pertenece a la entidad principal del dao
            relationship_to_join_value = getattr(class_to_check, field_to_check)

            # Busco el tipo de entidad para generar un alias. Utilizo el mapa de relaciones de la propia entidad.
            for att in class_to_check.__mapper__.relationships:
                rel_field_name = att.key
                if rel_field_name == field_to_check:
                    relationship_to_join_class = att.mapper.class_
                    break

            # Calculo el alias y lo añado al diccionario, siendo la clave el nombre del campo del join
            alias = aliased(relationship_to_join_class, name="_".join(sorted_element.join_split))
            # Añado un objeto al mapa para tener mejor controlados estos datos
            alias_dict[key] = _SQLModelHelper(model_type=relationship_to_join_class,
                                              model_alias=alias,
                                              model_owner_type=class_to_check,
                                              owner_breadcrumb=owner_breadcrumb,
                                              field_name=field_to_check,
                                              model_field_value=relationship_to_join_value)

        return join_clauses_sortened

    def __resolve_fields_info(self, aliases_dict: Dict[str, _SQLModelHelper],
                              clauses: Union[List[FilterClause],
                                             List[OrderByClause],
                                             List[FieldClause],
                                             List[GroupByClause]]) -> dict:
        """
        Resuelve la información de los campos para las cláusulas de filter, group by, order by y campos individuales.
        :param aliases_dict:
        :param clauses:
        :return: dicr
        """
        # Lo utilizo para separar el campo del filtro por los puntos y así obtener primero la entidad relacionada
        # (la lista hasta el último elemento sin incluir) y el nombre del campo por el que se va a filtrar. Lo
        # necesito para recuperar el alias del diccionario de alias, así como para tratar el tipo de dato por si
        # fuese por ejemplo una fecha.
        clause_split: List[str]
        entity_breadcrumb: str
        clause_entity: any
        field_alias: any
        field_to_work_with: any
        mapper: any

        # También voy a recuperar el tipo de campo por el que filtrar, lo voy a necesitar para tratar ciertos
        # tipos de filtros como por ejemplo filtro por fechas.
        field_type: any

        # Utilizo un namedtuple con los campos de alias, entidad, tipo de campo y el campo con el que se va a trabajar
        field_info = namedtuple("field_info", ["field_alias", "clause_entity", "field_type",
                                               "field_to_work_with"])

        field_info_dict: dict = {}

        for clause in clauses:
            if clause.field_name in field_info_dict:
                continue

            clause_split = clause.field_name.split(".")
            # Obtengo la entidad relacionada descartando el último elemento; se va a corresponder con la clave
            # del diccionario de alias
            entity_breadcrumb = ".".join(clause_split[:-1]) if len(clause_split) > 1 else None

            # El campo objetivo de la cláusula será siempre el último del split
            field_to_work_with = clause_split[-1]

            # En función de si es una entidad anidada, preparo los campos
            if entity_breadcrumb is None:
                # Si no existe miga de pan, es que no es una entidad anidada, la consulta se hace sobre la propia
                # entidad base.
                clause_entity = self.entity_type
                field_alias = None
            else:
                # Si existe miga de pan, es un filtro por algún campo anidado respecto a la entidad base; recupero
                # la información desde el diccionario de alias.
                if entity_breadcrumb not in aliases_dict:
                    raise ValueError(f"Unknown column {entity_breadcrumb} in clause {clause.field_name}")

                clause_entity = aliases_dict[entity_breadcrumb].model_type
                field_alias = aliases_dict[entity_breadcrumb].model_alias

            # Recupero el tipo de campo para tratar ciertos filtros especiales, como las fechas
            mapper = inspect(clause_entity)

            # Comprobar que existe el campo, si no existe lanzar excepción
            if field_to_work_with not in mapper.columns:
                raise AttributeError(f"There was not field {field_to_work_with} "
                                     f"in class {clause_entity.__name__}")

            field_type = mapper.columns[field_to_work_with].type

            # Obtengo el propio campo para filtrar
            # Si existe alias, hay que utilizarlo en los filtros (para el caso de entidades anidadas)
            if field_alias is not None:
                field_to_work_with = getattr(field_alias, field_to_work_with)
            else:
                field_to_work_with = getattr(clause_entity, field_to_work_with)

            # Añadir mapa con información del campo, siendo la clave el nombre del campo en la cláusula
            field_info_dict[clause.field_name] = field_info(field_alias=field_alias, field_type=field_type,
                                                            field_to_work_with=field_to_work_with,
                                                            clause_entity=clause_entity)

        return field_info_dict
