import os

from celery import shared_task
from celery import Task

from omegaml import Omega
from omegaml.documents import Metadata
from omegaml import signals


class NotebookTask(Task):
    abstract = True

    def on_success(self, retval, task_id, *args, **kwargs):
        args, kwargs = args[0:2]
        nbfile = args[0]
        metadata = Metadata.objects.get(
            name=nbfile, kind=Metadata.OMEGAML_RUNNING_JOBS)
        attrs = metadata.attributes
        attrs['state'] = 'SUCCESS'
        attrs['task_id'] = task_id
        metadata.kind = Metadata.OMEGAML_JOBS

        if not kwargs:
            pass
        else:
            attrs['last_run_time'] = kwargs.get('run_at')
            attrs['next_run_time'] = kwargs.get('next_run_time')

        metadata.attributes = attrs
        metadata.save()

    def on_failure(self, retval, task_id, *args, **kwargs):
        args, kwargs = args[0:2]
        nbfile = args[0]
        metadata = Metadata.objects.get(
            name=nbfile, kind=Metadata.OMEGAML_RUNNING_JOBS)
        attrs = metadata.attributes
        attrs['state'] = 'FAILURE'
        attrs['task_id'] = task_id
        metadata.kind = Metadata.OMEGAML_JOBS

        if not kwargs:
            pass
        else:
            attrs['last_run_time'] = kwargs.get('run_at')
            attrs['next_run_time'] = kwargs.get('next_run_time')

        metadata.attributes = attrs
        metadata.save()


class OmegamlTask(Task):
    abstract = True

    def on_success(self, retval, task_id, *args, **kwargs):
        signals.mltask_end.send(sender=None, state="SUCCESS", task=self.name)

    def on_failure(self, retval, task_id, *args, **kwargs):
        signals.mltask_end.send(sender=None, state="FAILURE", task=self.name)


def get_dataset_representations(items):
    """
    returns dict with x and y datasets
    """
    results = {}
    results['Xname'] = items.get('Xname')
    results['Yname'] = items.get('Yname')
    return results


@shared_task(base=OmegamlTask)
def omega_predict(modelname, Xname, rName=None, pure_python=True, **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.predict(modelname, Xname, rName, pure_python, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_predict',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_predict_proba(modelname, Xname, rName=None, pure_python=True,
                        **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.predict_proba(
        modelname, Xname, rName, pure_python, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_predict_proba',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_fit(modelname, Xname, Yname=None, pure_python=True, **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.fit(
        modelname, Xname, Yname, pure_python, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_fit',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_partial_fit(
        modelname, Xname, Yname=None, pure_python=True, **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.partial_fit(
        modelname, Xname, Yname, pure_python, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_partial_fit',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_score(modelname, Xname, Yname, rName=True, pure_python=True,
                **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.score(
        modelname, Xname, Yname, rName, pure_python, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_score',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_fit_transform(modelname, Xname, Yname=None, rName=None,
                        pure_python=True, **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.score(
        modelname, Xname, Yname, rName, pure_python, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_fit_transform',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_transform(modelname, Xname, rName=None, **kwargs):
    om = Omega()
    backend = om.models.get_backend(modelname)
    result = backend.transform(modelname, Xname, rName, **kwargs)
    signals.mltask_start.send(
        sender=None, name='omega_transform',
        args=get_dataset_representations(locals()))
    return result


@shared_task(base=OmegamlTask)
def omega_settings():
    if os.environ.get('OMEGA_DEBUG'):
        from omegaml.util import settings
        defaults = settings()
        return {k: getattr(defaults, k, '')
                for k in dir(defaults) if k and k.isupper()}
    return {'error': 'settings dump is disabled'}


@shared_task(base=NotebookTask)
def run_omegaml_job(nb_file):
    """
    retrieves the notebook from gridfs and runs it
    """
    om = Omega()
    result = om.jobs.run_notebook(nb_file)
    return result


@shared_task(base=NotebookTask)
def schedule_omegaml_job(nb_file, **kwargs):
    """
    retrieves the notebook from gridfs and runs it
    """
    om = Omega()
    result = om.jobs.run(nb_file)
    return result
