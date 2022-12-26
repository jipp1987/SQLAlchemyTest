import types
from typing import Callable, Dict, Type, List

from core.dao.daotools import FilterClause, JoinClause, OrderByClause, FieldClause, EnumAggregateFunctions, \
    GroupByClause
from core.dao.modelutils import BaseEntity, set_model_properties_by_dict
from core.exception.errorhandler import ErrorHandler


def service_method(function):
    """Utilizo este decorador para establecer un método de BaseService como método de transacción
    en la base de datos."""

    def wrapped(*args, **kwargs):
        # Esta parte sería para añadir alguna lógica en la función, pero no es el caso. Realmente la quiero devolver
        # como tal pero con un atributo a mayores para saber que le he añadido un decorador "service_function_dec". Lo
        # utilizo para saber qué funciones deben iniciar transacciones de base de datos y hacer rollback en caso de
        # error.
        pass

    # Le puedo añadir a cualquier función un atributo al vuelo, así que le añado uno para saber que tiene etiqueta
    function.is_service_method = True
    return function


class BaseService(object, metaclass=ErrorHandler):
    """
    Clase abstracta de la que han de heredar el resto de servicios del programa.
    """

    def __init__(self, dao):
        """
        Constructor.
        :param dao: DAO.
        """
        self._dao = dao
        """Acceso a datos asociado al servicio."""

    # Sobrescribo __getattribute__ para interceptar las llamadas a las funciones con el objetivo de envolverlas
    # automáticamente en transacciones, a modo de interceptor de llamadas a funciones.
    def __getattribute__(self, name):
        # Obtengo los atributos de la clase usando la función de la superclase object
        attr = object.__getattribute__(self, name)

        # Compruebo si el atributo es un método (una función de la clase, con parámetro "self"). También compruebo
        # si tiene un atributo llamado "is_service_method", lo cual significa que le he aplicado el decorador
        # "service_function_dec". Además, también compruebo que no haya sesión en el hilo actual: la idea es que sólo la
        # primera función de un service (sea cual sea) que inicie un proceso en la base de datos inicie la transacción;
        # cualquier otra función que de servicio (sea del mismo u otros) que tenga dentro de la función principal no
        # intente iniciar transacción, hacer commit o rollback si falla algo, que sólo lo haga la función principal.
        if isinstance(attr, types.MethodType) and getattr(attr, 'is_service_method', False) \
                and not self._dao.is_there_any_session_in_current_thread():
            # Creo una nueva función, que en realidad es la misma pero "envuelta" en la función de iniciar transacción
            def transaction_func(*args, **kwargs):
                result = self.__start_transaction(attr, *args, **kwargs)
                return result

            return transaction_func
        else:
            return attr

    def get_entity_type(self) -> type(BaseEntity):
        """
        Devuelve el tipo de entidad usando el dao asociado.
        :return: Tipo de entidad.
        """
        return self._dao.entity_type

    def __start_transaction(self, function: Callable, *args, **kwargs):
        """
        Envuelve la función dentro de un contexto transaccional: se hace commit al final si no hay problemas,
        y si sucede algo se hace rollback.
        :param function: Función miembro a ejecutar. Se accede a ella poniendo primero su nombre prececido de punto
        y el nombre del objeto al que pertenece
        :param args: Argumentos de la función que no se han identificado por clave.
        :param kwargs: Argumentos de la función que se han identificado por clave.
        :return: Resultado de la función
        """
        try:
            # Crear sesión
            self._dao.create_session()

            # Realizar función
            result = function(*args, **kwargs)

            # Hacer commit al final
            self._dao.commit()

            # Devolver resultado
            return result
        except Exception as e:
            # Si hay algún error, hacer rollback y devolver error hacia arriba.
            self._dao.rollback()
            raise e
        finally:
            # Desconectar siempre al final (sólo desconecta la función original, la que inició la transacción y solicitó
            # la conexión del hilo)
            self._dao.close_session()

    # FUNCIONES DE ACCESO A DATOS
    @service_method
    def create(self, registry: BaseEntity) -> None:
        """
        Crea una entidad en la base de datos y sincroniza su id.
        :param registry: Registro a crear.
        :return: None
        """
        self._dao.create(registry)

    @service_method
    def update(self, registry: BaseEntity) -> None:
        """
        Modifica una entidad en la base de datos. Modifica la entidad al completo, tal y como llega en el parámetro.
        :param registry: Registro a modificar.
        :return: None
        """
        self._dao.update(registry)

    @service_method
    def update_fields(self, registry_id: any, values_dict: dict) -> BaseEntity:
        """
        Modifica los campos de la entidad que son enviados en el diccionario de valores.
        :param registry_id: Id del registro a modificar.
        :param values_dict: Diccionario de valores a actualizar.
        :return: None
        """
        registry = self._prepare_entity_for_update_fields(registry_id, values_dict)

        # Actualizar la entidad
        self.update(registry)

        return registry

    def _prepare_entity_for_update_fields(self, registry_id: any, values_dict: dict) -> BaseEntity:
        # Busco la entidad a modificar por id.
        registry = self.find_by_id(registry_id)
        if registry is None:
            raise ValueError(f"There is not an entity of class {self.get_entity_type().__name__} with id "
                             f"{str(registry_id)}")

        # Modifico los datos enviados por parámetro en el diccionario, dejando el resto igual que estaban en la base de
        # datos.
        set_model_properties_by_dict(model_dict=values_dict, entity=registry, only_set_foreign_key=True)

        return registry

    @service_method
    def delete(self, registry: BaseEntity) -> None:
        """
        Elimina un registro por id.
        :param registry: Registro a eliminar.
        :return: None
        """
        self._dao.delete(registry)

    @service_method
    def load(self, registry_id: any) -> BaseEntity:
        """
        Función pensada para implementar en los servicios de aquellos modelos que tengan relaciones con otras tablas.
        Es un método de carga completa del objeto.
        :param registry_id: Id de la entidad.
        :return: Modelo cargado al completo.
        """
        return self.find_by_id(registry_id)

    @service_method
    def find_by_id(self, registry_id: any, join_clauses: List[JoinClause] = None):
        """
        Devuelve un registro a partir de un id.
        :param registry_id: Id del registro en la base de datos.
        :param join_clauses: Cláusulas join opcionales.
        :return: Una instancia de la clase principal del dao si el registro exite; None si no existe.
        """
        return self._dao.find_by_id(registry_id, join_clauses)

    @service_method
    def select(self, filter_clauses: List[FilterClause] = None, join_clauses: List[JoinClause] = None,
               order_by_clauses: List[OrderByClause] = None, limit: int = None, offset: int = None):
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
        return self._dao.select(filter_clauses=filter_clauses, join_clauses=join_clauses,
                                order_by_clauses=order_by_clauses, limit=limit, offset=offset)

    @service_method
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
        return self._dao.select_fields(filter_clauses=filter_clauses, join_clauses=join_clauses,
                                       order_by_clauses=order_by_clauses, field_clauses=field_clauses,
                                       group_by_clauses=group_by_clauses, limit=limit, offset=offset,
                                       return_raw_result=return_raw_result)

    @service_method
    def count_by_filtered_query(self, filter_clauses: List[FilterClause] = None,
                                join_clauses: List[JoinClause] = None) -> int:
        """
        Cuenta el número de registros de una tabla, pudiendo añadir filtros opcionales.
        :param filter_clauses: Filtros opcionales.
        :param join_clauses: Joins opcionales relacionados con los filtros.
        :return: int
        """
        field_label: str = f"count_{self._dao.get_entity_id_field_name()}"
        field_clauses: List[FieldClause] = [FieldClause(field_name=self._dao.get_entity_id_field_name(),
                                                        aggregate_function=EnumAggregateFunctions.COUNT,
                                                        field_label=field_label)]

        result = self._dao.select_fields(field_clauses=field_clauses, filter_clauses=filter_clauses,
                                         join_clauses=join_clauses, return_raw_result=True)

        # El resultado es una lista de tuplas: obtengo el primer valor del primer registro, que es donde está el
        # el recuento de registros.
        count: int = 0
        if result and result[0]:
            count = result[0][field_label]

        return count


class ServiceFactory(object):
    """
    Factoría de servicios, de tal modo que sólo se tendrá en el contexto de la aplicación una instancia de un servicio.
    """

    __services: Dict[str, BaseService] = {}
    """Diccionario de servicios ya instanciados. La clave es el nombre del servicio y el valor la instancia única del 
    mismo."""

    @classmethod
    def get_service(cls, class_to_instanciate: Type[BaseService]):
        """
        Devuelve una instancia de un servicio. Si no existe, la instancia y la guarda en el diccionario; si existe,
        devuelve la que tenga ya almacenada.
        :param class_to_instanciate: Clase del servicio a instanciar. Es el tipo, directamente, no el nombre.
        :return: Servicio.
        """
        # Obtengo el nombre de la clase
        class_name = class_to_instanciate.__name__

        # Si no existe, la creo y la almaceno en el diccionario
        if class_name not in cls.__services:
            cls.__services[class_name] = class_to_instanciate() # noqa

        # devuelvo la instancia del diccionario
        return cls.__services[class_name]
