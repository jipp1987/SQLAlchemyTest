from flask_cors import CORS
from flask_jwt_extended import JWTManager

from core.dao.basedao import BaseDao
from core.utils.fileutils import read_section_in_ini_file

from flask import Flask

from impl.rest.dbrestcontroller import db_service_blueprint
from impl.rest.userrestcontroller import user_service_blueprint

if __name__ == '__main__':
    # Configurar Dao desde fichero ini
    db_config = read_section_in_ini_file(file_name="config", section="DB")
    BaseDao.set_db_config_values(**db_config)

    # Configurar app
    app_config = read_section_in_ini_file(file_name="config", section="REST")

    # Aplicación flask
    app = Flask(__name__)

    # Configuración Json Web Token
    app.config["JWT_SECRET_KEY"] = read_section_in_ini_file(file_name="config", section="JWT")["jwt_secret_key"]
    jwt = JWTManager(app)

    # Registro de blueprints
    app.register_blueprint(db_service_blueprint)
    app.register_blueprint(user_service_blueprint)

    # CORS para habilitar llamadas cross-origin a la api
    CORS(app)

    # Ejecutar app
    app.run(**app_config)
