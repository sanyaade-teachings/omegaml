import dill
import os
import pandas as pd
import pkg_resources
import platform
from base64 import b64encode, b64decode
from datetime import datetime
from itertools import product
from uuid import uuid4

from omegaml.backends.basemodel import BaseModelBackend
from omegaml.store.documents import Metadata
from omegaml.util import _raise


class ExperimentBackend(BaseModelBackend):
    """ ExperimentBackend provides storage of tracker configurations

    Usage:

        To log metrics and other data::

            with om.runtime.experiment('myexp') as exp:
                om.runtime.model('mymodel').fit(X, Y)
                om.runtime.model('mymodel').score(X, Y) # automatically log score result
                exp.log_metric('mymetric', value)
                exp.log_param('myparam', value)
                exp.log_artifact(X, 'X')
                exp.log_artifact(Y, 'Y')
                exp.log_artifact(om.models.metadata('mymodel'), 'mymodel')

        To log data and automatically profile system data::

            with om.runtime.experiment('myexp', provider='profiling') as exp:
                om.runtime.model('mymodel').fit(X, Y)
                om.runtime.model('mymodel').score(X, Y) # automatically log score result
                exp.log_metric('mymetric', value)
                exp.log_param('myparam', value)
                exp.log_artifact(X, 'X')
                exp.log_artifact(Y, 'Y')
                exp.log_artifact(om.models.metadata('mymodel'), 'mymodel')

            # profiling data contains metrics for cpu, memory and disk use
            data = exp.data(event='profile')

        To get back experiment data without running an experiment::

            # recommended way
            exp = om.runtime.experiment('myexp').use()
            exp_df = exp.data()

            # experiments exist in the models store
            exp = om.models.get('experiments/myexp')
            exp_df = exp.data()

    See Also:

        * :class:`omegaml.backends.experiment.OmegaSimpleTracker`
        * :class:`omegaml.backends.experiment.OmegaProfilingTracker`
    """
    KIND = 'experiment.tracker'
    exp_prefix = 'experiments/'

    @classmethod
    def supports(self, obj, name, **kwargs):
        return isinstance(obj, TrackingProvider)

    def put(self, obj, name, **kwargs):
        name = f'{self.exp_prefix}{name}' if not name.startswith('experiments') else name
        # FIXME use proper pickle magic to avoid storing the store
        store = obj._store
        obj._store = None
        obj._model_store = None
        meta = super().put(obj, name, **kwargs)
        meta.attributes['dataset'] = obj._data_name
        meta.save()
        obj._store = store
        obj._model_store = self.model_store
        return meta

    def get(self, name, raw=False, data_store=None, **kwargs):
        assert data_store is not None, "experiments require a datastore, specify data_store=om.datasets"
        tracker = super().get(name, **kwargs)
        name = os.path.basename(name)
        tracker._store = data_store
        tracker._model_store = self.model_store
        return tracker.experiment(name) if not raw else tracker

    def drop(self, name, force=False, version=-1, data_store=None, **kwargs):
        data_store = data_store or self.data_store
        meta = self.model_store.metadata(name)
        dataset = meta.attributes.get('dataset')
        data_store.drop(dataset, force=True)
        return self.model_store._drop(name, force=force, version=version, **kwargs)


class TrackingProvider:
    """ TrackingProvider implements an abstract interface to experiment tracking

    Concrete implementations like MLFlow, Sacred or Neptune.ai can be implemented
    based on TrackingProvider. In combination with the runtime's OmegaTrackingProxy
    this provides a powerful tracking interface that scales with your needs.

    How it works:

        1. Experiments created using ``om.runtime.experiment()`` are stored as
           instances of a TrackingProvider concrete implementation

        2. Upon retrieval of an experiment, any call to its API is proxied to the
           actual implementation, e.g. MLFlow

        3. On calling a model method via the runtime, e.g. ``om.runtime.model().fit()``,
           the TrackingProvider information is passed on to the runtime worker,
           and made available as the backend.tracking property. Thus within a
           model backend, you can always log to the tracker by using::

                with self.tracking as exp:
                    exp.log_metric() # call any TrackingProvider method

        4. omega-ml provides the OmegaSimpleTracker, which implements a tracking
           interface similar to packages like MLFlow, Sacred. See ExperimentBackend
           for an example.
    """

    def __init__(self, experiment, store=None, model_store=None):
        self._experiment = experiment
        self._run = None
        self._store = store
        self._model_store = model_store

    def experiment(self, name=None):
        self._experiment = name or self._experiment
        return self

    def track(self, obj, store=None, label=None):
        """ attach this experiment to the named object

        Usage:

            # use in experiment context
            with om.runtime.experiment('myexp') as exp:
                exp.track('mymodel')

            # use on experiment directly
            exp = om.runtime.experiment('myexp')
            exp.track('mymodel')

        Args:
                obj (str): the name of the object
                store (OmegaStore): optional, om.models, om.scripts, om.jobs.
                    If not provided will use om.models
                label (str): optional, the label of the worker, default is
                    'default'

        Note:
                This modifies the object's metadata.attributes::

                    { 'tracking': { label: self._experiment }
        """
        label = label or 'default'
        store = store or self._model_store
        meta = store.metadata(obj)
        meta.attributes.setdefault('tracking', {})
        meta.attributes['tracking'] = {
            label: self._experiment
        }
        meta.save()
        return meta

    def active_run(self):
        self._run = (self._run or 0) + 1
        return self._run

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def log_event(self, event, key, value, step=None, **extra):
        raise NotImplementedError

    def log_metric(self, key, value, step=None, **extra):
        raise NotImplementedError

    def log_artifact(self, obj, name, step=None, **extra):
        raise NotImplementedError

    def log_param(self, key, value, step=None, **extra):
        raise NotImplementedError

    def tensorflow_callback(self):
        return TensorflowCallback(self)

    def data(self, experiment=None, run=None, event=None, step=None, key=None, raw=False):
        raise NotImplementedError

    @property
    def _data_name(self):
        return f'.experiments/{self._experiment}'


class NoTrackTracker(TrackingProvider):
    """ A default tracker that does not record anything """

    def start(self):
        pass

    def stop(self):
        pass

    def log_artifact(self, obj, name, step=None, **extra):
        pass

    def log_metric(self, key, value, step=None, **extra):
        pass

    def log_param(self, key, value, step=None, **extra):
        pass

    def log_event(self, event, key, value, step=None, **extra):
        pass

    def data(self, experiment=None, run=None, event=None, step=None, key=None, raw=False):
        pass


class OmegaSimpleTracker(TrackingProvider):
    """ A tracking provider that logs to an omegaml dataset

    Usage::

        with om.runtime.experiment(provider='default') as exp:
            ...
            exp.log_metric('accuracy', .78)
    """
    _provider = 'simple'
    _experiment = None
    _startdt = None
    _stopdt = None

    _ensure_active = lambda self, r: r if r is not None else _raise(ValueError('no active run, call .start() or .use() '))

    def active_run(self):
        """ set the lastest run as the active run

        Returns:
            current run (int)
        """
        self._run = self._latest_run or self.start()
        self._experiment = self._experiment or uuid4().hex
        return self._run

    def use(self):
        """ reuse the latest run instead of starting a new one

        semantic sugar for self.active_run()

        Returns:
            self
        """
        self.active_run()
        return self

    @property
    def _latest_run(self):
        data = self.data(event='start', raw=True)
        run = data[-1]['run'] if data is not None and len(data) > 0 else None
        return run

    @property
    def status(self, run=None):
        """ status of a run

        Args:
            run (int): the run number, defaults to the currently active run

        Returns:
            status in 'STARTED', 'STOPPED'
        """
        data = self.data(event=('start', 'stop'), run=run, raw=True)
        return 'STARTED' if len(data) > 1 else 'STOPPED'

    def start(self):
        """ start a new run

        This starts a new run and logs the start event
        """
        self._run = (self._latest_run or 0) + 1
        self._startdt = datetime.utcnow()
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'event': 'start',
            'dt': self._startdt,
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        self._save_data(data, self._data_name)
        self.log_system()
        return self._run

    def stop(self):
        """ stop the current run

        This stops the current run and records the stop event
        """
        self._stopdt = datetime.utcnow()
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'event': 'stop',
            'dt': self._stopdt,
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        self._save_data(data, self._data_name)

    def _save_data(self, data, dsname):
        self._store.put(data, dsname, noversion=True,
                        kind='pandas.rawdict')

    def log_artifact(self, obj, name, step=None, **extra):
        """ log any object to the current run

        Usage::

            # log an artifact
            exp.log_artifact(mydict, 'somedata')

            # retrieve back
            mydict_ = exp.restore_artifact('somedata')

        Args:
            obj (obj): any object to log
            name (str): the name of artifact
            step (int): the step, if any
            **extra: any extra data to log

        Notes:
            * bool, str, int, float, list, dict are stored as ``format=type``
            * Metadata is stored as ``format=metadata``
            * objects supported by ``om.models`` are stored as ``format=model``
            * objects supported by ``om.datasets`` are stored as ``format=dataset``
            * all other objects are pickled and stored as ``format=pickle``
        """
        if isinstance(obj, (bool, str, int, float, list, dict)):
            format = 'type'
            rawdata = obj
        elif isinstance(obj, Metadata):
            format = 'metadata'
            rawdata = obj.to_json()
        elif self._model_store.get_backend_byobj(obj) is not None:
            objname = uuid4().hex
            meta = self._model_store.put(obj, f'.experiments/.artefacts/{objname}')
            format = 'model'
            rawdata = meta.name
        elif self._store.get_backend_by_obj(obj) is not None:
            objname = uuid4().hex
            meta = self._model_store.put(obj, f'.experiments/.artefacts/{objname}')
            format = 'dataset'
            rawdata = meta.name
        else:
            try:
                rawdata = b64encode(dill.dumps(obj)).decode('utf8')
                format = 'pickle'
            except TypeError as e:
                rawdata = repr(obj)
                format = 'repr'
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'step': step,
            'event': 'artifact',
            'key': name,
            'value': {
                'name': name,
                'data': rawdata,
                'format': format
            },
            'dt': datetime.utcnow(),
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        self._save_data(data, self._data_name)

    def log_event(self, event, key, value, step=None, dt=None, **extra):
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'step': step,
            'event': event,
            'key': key,
            'value': value,
            'dt': dt or datetime.utcnow(),
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        if step is not None:
            data['step'] = step
        data.update(extra)
        self._save_data(data, self._data_name)

    def log_param(self, key, value, step=None, dt=None, **extra):
        """ log an experiment parameter

        Args:
            key (str): the parameter name
            value (str|float|int|bool|dict): the parameter value
            step (int): the step
            **extra: any other values to store with event

        Notes:
            * logged as ``event=param``
        """
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'step': step,
            'event': 'param',
            'key': key,
            'value': value,
            'dt': dt or datetime.utcnow(),
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        if step is not None:
            data['step'] = step
        data.update(extra)
        self._save_data(data, self._data_name)

    def log_metric(self, key, value, step=None, dt=None, **extra):
        """ log a metric value

        Args:
            key (str): the metric name
            value (str|float|int|bool|dict): the metric value
            step (int): the step
            **extra: any other values to store with event

        Notes:
            * logged as ``event=metric``
        """
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'step': step,
            'event': 'metric',
            'key': key,
            'value': value,
            'dt': dt or datetime.utcnow(),
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        if step is not None:
            data['step'] = step
        data.update(extra)
        self._save_data(data, self._data_name)

    def log_system(self, key=None, value=None, step=None, dt=None, **extra):
        """ log system data

        Args:
            key (str): the key to use, defaults to 'system'
            value (str|float|int|bool|dict): the parameter value
            step (int): the step
            **extra: any other values to store with event

        Notes:
            * logged as ``event=system``
            * logs platform, python version and list of installed packages
        """
        key = key or 'system'
        value = value or {
            'platform': platform.uname()._asdict(),
            'python': '-'.join((platform.python_implementation(),
                                platform.python_version())),
            'packages': ['=='.join((d.project_name, d.version))
                         for d in pkg_resources.working_set]
        }
        data = {
            'experiment': self._experiment,
            'run': self._ensure_active(self._run),
            'step': step,
            'event': 'system',
            'key': key,
            'value': value,
            'dt': dt or datetime.utcnow(),
            'node': os.environ.get('HOSTNAME', platform.node()),
        }
        data.update(extra)
        self._save_data(data, self._data_name)

    def data(self, experiment=None, run=None, event=None, step=None, key=None, raw=False):
        """ build a dataframe of all stored data

        Args:
            experiment (str): the name of the experiment, defaults to its current value
            run (int|list): the run(s) to get data back, defaults to current run, use 'all' for all
            event (str|list): the event(s) to include
            step (int|list): the step(s) to include
            key (str|list): the key(s) to include
            raw (bool): if True returns the raw data instead of a DataFrame

        Returns:
            * data (DataFrame) if raw == False
            * data (list of dicts) if raw == True
        """
        filter = {}
        experiment = experiment or self._experiment
        run = run or self._run
        valid = lambda s: s is not None and str(s).lower() != 'all'
        op = lambda s: {'$in': list(s)} if isinstance(s, (list, tuple)) else s
        if valid(experiment):
            filter['experiment'] = op(experiment)
        if valid(run):
            filter['run'] = op(run)
        if valid(event):
            filter['event'] = op(event)
        if valid(step):
            filter['step'] = op(step)
        if valid(key):
            filter['key'] = op(key)
        data = self._store.get(self._data_name, filter=filter, parser=None,
                               resolve='value' if not raw else 'to_dict')
        if data is not None and not raw:
            data.sort_values('dt', inplace=True) if 'dt' in data.columns else None
            data = data.to_dict() if data is not None and raw else data
        return data

    def restore_artifact(self, key=None, experiment=None, run=None, step=None, value=None):
        """ restore a logged artificat

        Args:
            key (str): the name of the artifact as provided in log_artifact
            run (int): the run for which to query, defaults to current run
            step (int): the step for which to query, defaults to all steps in run
            value (dict): this value is used instead of querying data, use to
               retrieve an artifact from contents of ``.data()``

        Notes:
            * this will restore the artifact according to its type assigned
              by ``.log_artifact()``. If the type cannot be determined, the
              actual data is returned
        """
        if value is None:
            data = self.data(experiment=experiment, run=run, event='artifact', step=step, key=key, raw=True)
            data = data[-1]['value']
        else:
            data = value
        if data['format'] == 'type':
            obj = data['data']
        elif data['format'] == 'metadata':
            meta = self._store._Metadata
            obj = meta.from_json(data['data'])
        elif data['format'] == 'dataset':
            obj = self._store.get(data['data'])
        elif data['format'] == 'model':
            obj = self._model_store.get(data['data'])
        elif data['format'] == 'pickle':
            obj = dill.loads(b64decode((data['data']).encode('utf8')))
        else:
            obj = data.get('data', data)
        return obj


class OmegaProfilingTracker(OmegaSimpleTracker):
    """ A metric tracker that runs a system profiler while the experiment is active

    Will record ``profile`` events that contain cpu, memory and disk profilings.
    See BackgroundProfiler.profile() for details of the profiling metrics collected.

    Usage:

        To log metrics and system performance data::

            with om.runtime.experiment('myexp', provider='profiling') as exp:
                ...

            data = exp.data(event='profile')

    Properties::

        exp.profiler.interval = n.m # interval of n.m seconds to profile, defaults to 3 seconds
        exp.profiler.metrics = ['cpu', 'memory', 'disk'] # all or subset of metrics to collect
        exp.max_buffer = n # number of items in buffer before tracking

    Notes:
        - the profiling data is buffered to reduce the number of database writes, by
          default the data is written on every 6 profiling events (default: 6 * 10 = every 60 seconds)
        - the step reported in the tracker counts the profiling event since the start, it is not
          related to the step (epoch) reported by e.g. tensorflow
        - For every step there is a ``event=profile``, ``key=profile_dt`` entry which you can use
          to relate profiling events to a specific wall-clock time.
        - It usually sufficient to report system metrics in intervals > 10 seconds since machine
          learning algorithms tend to use CPU and memory over longer periods of time.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.profile_logs = []
        self.max_buffer = 6

    def log_profile(self, data):
        """ the callback for BackgroundProfiler """
        self.profile_logs.append(data)
        if len(self.profile_logs) >= (self.max_buffer or 1):
            self.flush()

    def flush(self):
        for step, data in enumerate(self.profile_logs):
            # record the actual time instead of logging time (avoid buffering delays)
            dt = data.get('profile_dt')
            for k, v in data.items():
                self.log_event('profile', k, v, step=step, dt=dt)
        self.profile_logs = []

    def start(self):
        self.profiler = BackgroundProfiler(callback=self.log_profile)
        self.profiler.start()
        super().start()

    def stop(self):
        self.profiler.stop()
        self.flush()
        super().stop()


try:
    from tensorflow import keras
except:
    pass
else:
    class TensorflowCallback(keras.callbacks.Callback):
        """ A callback for Tensorflow Keras models

        Implements the callback protocol according to Tensorflow Keras
        semantics and linking to a :class:`omegaml.backends.experiment.TrackingProvider`

        See Also:

            * https://www.tensorflow.org/guide/keras/custom_callback
        """
        #
        def __new__(cls, *args, **kwargs):
            # generate methods as per specs
            for action, phase in product(['train', 'test', 'predict'], ['begin', 'end']):
                cls.wrap(f'on_{action}_{phase}', 'on_global')
                cls.wrap(f'on_{action}_batch_{phase}', 'on_batch')
            for phase in ['begin', 'end']:
                cls.wrap(f'on_epoch_{phase}', 'on_epoch')
            return super().__new__(cls)

        def __init__(self, tracker):
            self.tracker = tracker
            self.model = None

        def set_model(self, model):
            self.tracker.log_artifact(model, 'model')

        def set_params(self, params):
            for k, v in params.items():
                self.tracker.log_param(k, v)

        def on_global(self, action, logs=None):
            for k, v in (logs or {}).items():
                self.tracker.log_metric(k, v, step=0)

        def on_batch(self, action, batch, logs=None):
            for k, v in (logs or {}).items():
                self.tracker.log_metric(k, v, step=batch)

        def on_epoch(self, action, epoch, logs=None):
            for k, v in (logs or {}).items():
                self.tracker.log_metric(k, v, step=epoch)

        @classmethod
        def wrap(cls, method, fn):
            fn = getattr(cls, fn)

            def inner(self, *args, **kwargs):
                return fn(self, method, *args, **kwargs)

            setattr(cls, method, inner)
            return inner


class BackgroundProfiler:
    """ Profile CPU, Memory and Disk use in a background thread
    """

    def __init__(self, interval=10, callback=print):
        self._stop = False
        self._interval = interval
        self._callback = callback
        self._metrics = ['cpu', 'memory', 'disk']

    def profile(self):
        """
        returns memory and cpu data as a dict
            memory_load (float): percent of total memory used
            memory_total (float): total memory in bytes
            cpu_load (list): cpu load in percent, per cpu
            cpu_count (int): logical cpus,
            cpu_freq (float): MHz of current cpu frequency
            cpu_avg (list): list of cpu load over last 1, 5, 15 minutes
            disk_use (float): percent of disk used
            disk_total (int): total size of disk, bytes
        """
        import psutil
        from datetime import datetime as dt
        p = psutil
        disk = p.disk_usage('/')
        cpu_count = p.cpu_count()
        data = {'profile_dt': dt.utcnow()}
        if 'memory' in self._metrics:
            data.update(memory_load=p.virtual_memory().percent,
                        memory_total=p.virtual_memory().total)
        if 'cpu' in self._metrics:
            data.update(cpu_load=p.cpu_percent(percpu=True),
                        cpu_count=cpu_count,
                        cpu_freq=[f.current for f in p.cpu_freq(percpu=True)],
                        cpu_avg=[x / cpu_count for x in p.getloadavg()])
        if 'disk' in self._metrics:
            data.update(disk_use=disk.percent,
                        disk_total=disk.total)
        return data

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def interval(self, interval):
        self._interval = interval
        self.stop()
        self.start()

    @property
    def metrics(self):
        return self._metrics

    @metrics.setter
    def metrics(self, metrics):
        self._metrics = metrics

    def start(self):
        """ runs a background thread that reports stats every interval seconds

        Every interval, calls callback(data), where data is the output of BackgroundProfiler.profile()
        Stop by BackgroundProfiler.stop()
        """
        import atexit
        from threading import Thread
        from time import sleep

        def runner():
            cb = self._callback
            try:
                while not self._stop:
                    cb(self.profile())
                    sleep(self._interval)
            except (KeyboardInterrupt, SystemExit):
                pass

        # handle exits by stopping the profiler
        atexit.register(self.stop)
        # start the profiler
        self._stop = False
        t = Thread(target=runner)
        t.start()
        # clean up
        atexit.unregister(self.stop)

    def stop(self):
        self._stop = True
