from sklearn.externals.joblib import Parallel, delayed

from omegaml.util import PickableCollection


class ParallelApplyMixin:
    """
    enables parallel apply to MDataFrame
    """
    def transform(self, fn=None, inplace=False, preparefn=None, n_jobs=-2, maxobs=None,
                  chunksize=50000, verbose=100, chunkfn=None, outname=None,
                  resolve='worker'):
        mdf = self.__class__(self.collection, **self._getcopy_kwargs())
        self._pyapply_opts = getattr(self, '_pyapply_opts', {})
        self._pyapply_opts.update({
            'maxobs': maxobs or len(self),
            'n_jobs': n_jobs,
            'chunksize': chunksize,
            'applyfn': fn or pyappply_nop_transform,
            'chunkfn': chunkfn,
            'mdf': mdf,
            'append': False,
            'outname': outname or '_tmp{}_'.format(mdf.collection.name),
            'resolve': resolve,  # worker or function
        })
        return self

    def _chunker(self, mdf, chunksize, maxobs):
        if getattr(mdf.collection, 'query', None):
            for i in range(0, maxobs, chunksize):
                yield mdf.skip(i).head(i + chunksize)
        else:
            for i in range(0, maxobs, chunksize):
                yield mdf.iloc[i:i + chunksize]

    def _do_transform(self, verbose=0):
        # setup mdf and parameters
        opts = self._pyapply_opts
        n_jobs = opts['n_jobs']
        chunksize = opts['chunksize']
        applyfn = opts['applyfn']
        chunkfn = opts['chunkfn'] or self._chunker
        maxobs = opts['maxobs']
        mdf = opts['mdf']
        outname = opts['outname']
        append = opts['append']
        resolve = opts['resolve']
        # prepare for serialization to remote worker
        outcoll = PickableCollection(mdf.collection.database[outname])
        if not append:
            outcoll.drop()
        # run in parallel
        chunks = chunkfn(mdf, chunksize, maxobs)
        runner = delayed(pyapply_process_chunk)
        worker_resolves_mdf = resolve in ('worker', 'w')
        jobs = [runner(mdf, i, chunksize, applyfn, outcoll, worker_resolves_mdf)
                for i, mdf in enumerate(chunks)]
        with Parallel(n_jobs=n_jobs, backend='omegaml',
                      verbose=verbose) as p:
            p._backend._job_count = len(jobs)
            if verbose:
                print("Submitting {} tasks".format(len(jobs)))
            p(jobs)
        return outcoll

    def _get_cursor(self, pipeline=None, use_cache=True):
        if getattr(self, '_pyapply_opts', None):
            result = self._do_transform().find()
        else:
            result = super()._get_cursor(pipeline=pipeline, use_cache=use_cache)
        return result

    def persist(self, name=None, store=None, append=False, local=False):
        self._pyapply_opts = getattr(self, '_pyapply_opts', {})
        # -- .transform() active
        if self._pyapply_opts:
            meta = None
            if name and store:
                coll = store.collection(name)
                if coll.name == self.collection.name:
                    raise ValueError('persist() must be to a different collection than already existing')
                try:
                    if not append:
                        store.drop(name, force=True)
                    meta = store.put(coll, name)
                except:
                    print("WARNING please upgrade omegaml to support accessing collections")
                else:
                    # _do_transform expects the collection name, not the store's name
                    name = coll.name
            self._pyapply_opts.update(dict(outname=name, append=append))
            coll = self._do_transform()
            result = meta or self.__class__(coll, **self._getcopy_kwargs())
        # -- run with noop in parallel
        elif not local and getattr(self, 'apply_fn', None) is None and name:
            # convenience, instead of .value call mdf.persist('name', store=om.datasets)
            result = self.transform().persist(name=name, store=store, append=append)
        # -- some other action is active, e.g. groupby, apply
        elif local or (name and store):
            print("warning: resolving the result of aggregation locally before storing")
            value = self.value
            result = store.put(value, name, append=append)
        else:
            result = super().persist()
        return result


def pyappply_nop_transform(ldf):
    # default transform that does no transformations
    pass


def pyapply_process_chunk(mdf, i, chunksize, applyfn, outcoll, resolve):
    # chunk processor
    import pandas as pd
    from inspect import signature
    # fix pickling issues
    mdf._parser = getattr(mdf, '_parser', None)
    mdf._raw = getattr(mdf, '_raw', None)
    # check apply fn so we can pass the right number of args
    sig = signature(applyfn)
    params = sig.parameters
    try:
        if resolve:
            # requested to resolve value before passing on
            chunkdf = mdf.value
        else:
            # requested to pass on mdf itself
            chunkdf = mdf
    except Exception as e:
        raise e
        raise RuntimeError(f".value on {mdf} cause exception {e})")
    else:
        applyfn_args = [chunkdf, i][0:len(params)]
    # call applyfn
    if len(chunkdf):
        try:
            result = applyfn(*applyfn_args)
        except Exception as e:
            raise RuntimeError(e)
        else:
            chunkdf = result if result is not None else chunkdf
            if isinstance(chunkdf, pd.Series):
                chunkdf = pd.DataFrame(chunkdf,
                                       index=chunkdf.index,
                                       columns=[str(chunkdf.name)])
        start = i * chunksize
        if chunkdf is not None and len(chunkdf):
            end = start + len(chunkdf)
            chunkdf['_om#rowid'] = pd.RangeIndex(start, end)
            outcoll.insert_many(chunkdf.to_dict(orient='records'))
