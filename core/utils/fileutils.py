import configparser
import os.path


def get_project_root_dir() -> str:
    """
    Función para obtener el directorio raíz del proyecto. OJO!!! Sólo se ha testeado su correcto funcionamiento cuando
    el nombre del fichero principal del proyecto es "main.py".
    :return: String con la ruta del directorio raíz.
    """
    # Busco el main del proyecto.
    import __main__ as main # noqa

    if main:
        # Compruebo si main tiene el atributo __file__. Este atributo es la ruta del archivo desde el que se ha cargado
        # el módulo, en caso de que se haya cargado desde un fichero.
        if hasattr(main, '__file__'):
            # Si lo tiene, busco la ruta absoluta del mismo
            script_name = os.path.abspath(os.path.join(os.getcwd(), main.__file__))
            root_dir = os.path.dirname(script_name)
        else:
            # Si no lo tiene, devuelvo el directorio de trabajo actual
            root_dir = os.getcwd()
    else:
        root_dir = os.getcwd()

    return root_dir


def read_section_in_ini_file(file_name: str, section: str, file_path: str = None) -> dict:
    """
    Lee un fichero .ini y devuelve un diccionario con el contenido de la sección pasada como parámetro.
    :param file_name: Nombre del fichero sin la ruta. Si no incluye la extensión, se le añade dentro de esta función.
    :param section: Nombre de la sección a leer.
    :param file_path: Ruta del fichero sin incluir su nombre. Si no se especifica, se considerará que la ruta es el
    directorio "resources" de la raíz del proyecto.
    :return: dict
    """
    # Preparo el parseador de ficheros
    config = configparser.ConfigParser()

    # Si no se ha especificado una ruta para el fichero, asumo que es la raíz del proyecto
    if file_path is None:
        file_path = os.path.join(get_project_root_dir(), 'resources')

    # Comprobar si el fichero finaliza en .ini. Si no lo hace se añade la extensión.
    if not file_name.endswith(".ini"):
        file_name += ".ini"

    # Añado un slash al final del file_path si no lo tiene ya.
    if not file_path.endswith("/"):
        file_path += "/"

    # Ruta completa del fichero a leer
    full_path: str = file_path + file_name

    # Si no existe el fichero, lanzar excepción.
    if not os.path.exists(full_path):
        raise Exception(f'File \"{full_path}\" does not exist.')

    # Lectura del fichero
    config.read(full_path)

    # Comprobar que la sección existe en el fichero.
    if section not in config.sections():
        raise Exception(f'Section \"{section}\" does not exist in .ini file {file_name}')

    # Creo un diccionario para devolver el contenido de la sección pasada como parámetro.
    section_dict: dict = {}
    for key, value in config[section].items():
        section_dict[key] = value

    return section_dict
