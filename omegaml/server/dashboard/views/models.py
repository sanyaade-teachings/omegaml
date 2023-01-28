from flask import render_template, request


def create_view(bp):
    @bp.route('/model/<path:name>')
    def detail(name):
        template = 'model_detail.html'
        if request.method == 'POST':
            handle_update(name)
        data = {
            'name': name,
            'kind': 'sklearn',
            'text': 'lorem impsum',
            'attributes': {
                'foo': 'bar',
            }
        }
        return render_template(f"dashboard/{template}", segment='models', buckets=['default'], **data)

    def handle_update(name):
        pass


