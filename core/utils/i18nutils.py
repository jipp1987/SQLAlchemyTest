import gettext
import locale
from typing import List, Dict

from core.utils.fileutils import get_project_root_dir


def prepare_translations(language_list: List[str], mo_file_name: str, dir_name: str) -> Dict[str, any]:
    """
    Prepara los diccionarios de internacionalización i18n.
    :param language_list: Lista de strings con los isos de los idiomas: es_ES, en_GB...
    :param mo_file_name: Nombre del fichero mo sin la extensión. Debería llamarse igual en todas las carpetas.
    :param dir_name: Ruta del directorio (sin separador final) desde la raíz del proyecto en el que se encuentran
    los ficheros .mo. Dentro de esta ruta debería haber una carpeta llamada "locales" donde estará la estructura i18n,
    en el nombre del directorio NO hay que incluir esa palabra.
    :return: Dict[str, any]
    """
    translations: Dict[str, any] = {}

    file_path: str = f'{get_project_root_dir()}/{dir_name}/locales'
    for iso_key in language_list:
        translations[iso_key] = gettext.translation(mo_file_name, file_path, languages=[iso_key], fallback=True)

    return translations


def change_locale(locale_iso: str):
    """Cambia el locale del sistema.
    :param locale_iso: Código Iso del locale al que se quiere cambiar
    """
    locale.setlocale(locale.LC_ALL, locale_iso)


def translate(key: str, languages: Dict[str, any], locale_iso: str = None, args: list = None) -> str:
    """
    Traduce una clave i18n.
    :param key: Clave i18n a traducir.
    :param languages: Diccionario de idiomas que se va a utilizar.
    :param locale_iso: Iso objetivo.
    :param args: Lista de parámetros para aquellas claves que tengan sustitución de variables.
    :return: Clave i18n traducida.
    """
    # Primero obtengo el valor de la clave en los ficheros po/mo
    # Locale es una tupla, el primer valor es el código del idioma que es lo que uso en como clave del diccionario
    result = languages[locale_iso if locale_iso is not None else locale.getlocale()[0]].gettext(key)

    # Ahora, si han llegado parámetros para sustituir los placeholders, los sustituyo en el valor obtenido
    if args:
        result = result % (*args,)

    return result
