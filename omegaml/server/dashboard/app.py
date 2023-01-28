from flask import Blueprint

omega_bp = Blueprint('omega-server', __name__, template_folder='templates')

from omegaml.server.dashboard.views import dashboard
from omegaml.server.dashboard.views import models

dashboard.create_view(omega_bp)
models.create_view(omega_bp)
