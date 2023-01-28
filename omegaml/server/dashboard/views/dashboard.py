from dataclasses import dataclass

from flask import Blueprint, render_template
from flask_login import login_required

omega_bp = Blueprint('dashboard', __name__, template_folder='templates')


@dataclass
class Metadata:
    name: str


buckets = ['default']


def create_view(bp):
    @bp.route('/index')
    def index():
        return render_template('dashboard/index.html', segment='index', buckets=buckets)


    @bp.route('/<path:module>')
    @login_required
    def list(module):
        template = f'{module}.html' if not module.endswith('.html') else module
        models = [
                     Metadata(name='foo')
                 ] * 100
        return render_template(f"dashboard/{template}", segment=module, models=models, buckets=buckets)


    @bp.route('/models/<path:name>')
    @login_required
    def models(name):
        return


