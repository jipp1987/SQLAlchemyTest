from flask_cors import CORS

from core.dao.basedao import BaseDao
from core.utils.fileutils import read_section_in_ini_file

from flask import Flask

from impl.rest.restcontrollerimpl import db_service_blueprint

if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    db_config = read_section_in_ini_file(file_name="config", section="DB")
    BaseDao.set_db_config_values(**db_config)

    # Configurar app
    app_config = read_section_in_ini_file(file_name="config", section="REST")

    # Aplicación flask
    app = Flask(__name__)

    # Registro de blueprints
    app.register_blueprint(db_service_blueprint)

    # CORS para habilitar llamadas cross-origin a la api
    CORS(app)

    # Ejecutar app
    app.run(**app_config)
