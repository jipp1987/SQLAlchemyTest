import abc
from collections import namedtuple
import threading
from copy import deepcopy
from typing import Dict, List, Union

from sqlalchemy import create_engine, select, and_, or_, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, declarative_base, contains_eager, aliased
from sqlalchemy.sql import expression

from core.dao.daotools import FilterClause, EnumFilterTypes, EnumOperatorTypes, JoinClause, EnumJoinTypes, \
    OrderByClause, GroupByClause, EnumOrderByTypes, EnumSQLEngineTypes, _SQLModelHelper

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
    def select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None,
               order_by_clauses: List[OrderByClause] = None) \
            -> List[BaseEntity]:
        """
        Selecciona entidades cargadas con todos sus campos. Si se incluyem joins con fetch, traerá cargadas también
        las entidades anidadas referenciadas en los joins.
        :param filter_clauses:
        :param join_clauses:
        :param order_by_clauses:
        :return: List[BaseEntity]
        """
        return self.__select(filter_clauses=filter_clauses, join_clauses=join_clauses,
                             order_by_clauses=order_by_clauses)

    def __select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None,
                 order_by_clauses: List[OrderByClause] = None) \
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
            # Esta función devuelve la lista de joins ordenada de acuerdo a su nivel de "anidación" de entidades,
            # para respetar un orden lógico de joins y evitar resultados duplicados y equívocos en la consulta.
            join_clauses = self.__resolve_field_aliases(join_clauses=join_clauses, alias_dict=aliases_dict)

        # Expresión de la consulta
        stmt = select(self.entity_type)

        # Resolver cláusula join
        if join_clauses:
            stmt = self.__resolve_join_clause(join_clauses=join_clauses, stmt=stmt, alias_dict=aliases_dict)

        # Resolver cláusula where
        if filter_clauses:
            stmt = self.__resolve_filter_clauses(filter_clauses=filter_clauses, stmt=stmt, alias_dict=aliases_dict)

        # Resolver cláusula order by
        if order_by_clauses:
            stmt = self.__resolve_order_by_clauses(order_by_clauses=order_by_clauses, stmt=stmt,
                                                   alias_dict=aliases_dict)

        # Ejecutar la consulta
        result = my_session.execute(stmt).scalars().all()

        # Para evitar problemas, hago flush y libero todos los elementos
        my_session.flush()
        my_session.expunge_all()

        return result

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
            filter_expression: any = None

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
                field_type = field_info.field_type

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
                    expression_for_nested_filter = f_operator(filter_expression,
                                                              __inner_resolve_filter_clauses(f.related_filter_clauses,
                                                                                             field_info_dict_inner)) \
                        .self_group()
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
    def __resolve_join_clause(join_clauses: List[JoinClause], stmt, alias_dict: Dict[str, _SQLModelHelper]):
        """
        Resuelve la cláusula join.
        :param join_clauses: Lista de cláusulas join.
        :param alias_dict: Diccionario de alias.
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
            # Recuepero el valor del join, el campo del modelo por el que se va a hacer join
            relationship_to_join = alias_dict[j.field_name].model_field_value

            # Recupero el alias calculated anteriormente
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
            if j.is_join_with_fetch:
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
        previous_key: str
        """Clave anterior para la construcción de la miga de pan."""
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

        # Ordenar la lista en función del número de elementos
        join_sorted_list = sorted(join_sorted_list, key=lambda x: len(x.join_split), reverse=False)

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
            class_to_check = self.entity_type

            # Primero intento recuperar el valor del mapa, para así obtener los datos del elemento inmediatamente
            # anterior. La clave a recuperar no es la actual, sino la del elemento anterior, para lo cual tengo que
            # acceder al penúltimo nivel del array
            if len(sorted_element.join_split) > 1:
                previous_key = ".".join(sorted_element.join_split[:-1])
                class_to_check = alias_dict[previous_key].model_type

            # Si no es el caso, asumimos que pertenece a la entidad principal del dao
            relationship_to_join_value = getattr(class_to_check, field_to_check)

            # Esto lo necesito porque si es una entidad anidad sobre otra entidad anidada, necesito toda
            # la "miga de pan" para que el join funcione correctamente, si sólo especifico el último valor no entenderá
            # de dónde viene la entidad.
            owner_breadcrumb = []
            if len(sorted_element.join_split) > 1:
                previous_key: str = ".".join(sorted_element.join_split[:-1])
                # Primero añado la lista que ya tuviera el propietario, a modo de miga de pan
                owner_breadcrumb.extend(alias_dict[previous_key].owner_breadcrumb)
                # Luego añado la que le corresponde a sí mismo, que es la del registro anterior.
                owner_breadcrumb.append((alias_dict[previous_key].model_field_value,
                                         alias_dict[previous_key].model_alias))

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
