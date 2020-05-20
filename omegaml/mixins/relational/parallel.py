from joblib import Parallel, delayed

"""
lazy.transform(func).persist('name')

process 1:
    result = lazy[0:chunksize].value
    om.datasets.put(result, name)
    
process 2:
    result = lazy[chunksize:2*chunksize].value
    om.datasets.put(result, name)
    
in general:

process i:
    result = store.getl(source).filter(chunk-spec[i])
    store.put(result, name) 
    
.transform() => needs to create the transform specification
.persist() => run the transform and persist each chunk 

    
Transform process:

with Parallel() as p:
    runner = delayed(transform-fn)
    chunks = chunker()
    jobs = [runner(i, ...) for i in chunks] 
    p(jobs)
"""


class ParallelMixin:
    """
    Enables parallel transform to MRelational

    This enables Python-native Pandas processing to be applied to MRelational
    of any size

    Usage:
        def myfunc(df):
            # this function is executed for each chunk in parallel
            # use any Pandas DataFrame native functions
            df[...] = df.apply(...)

        om.datasets.getl('verylarge').transform(myfunc).persist('transformed')

    Notes:

        * technically, .transform() is the equivalent of

          Parallel()(myfunc(chunk) for chunk in chunker('verylarge'))()

          where chunker() is a function that returns a list of serializable
          chunks and passes each chunk to myfunc as a resolvable DataFrame.

        * .persist() is the equivalent of result = myfunc(chunk) => store.put(result)
          where myfunc(chunk) returns a dataframe that is saved using store.put
    """

    def _init_mixin(self, *args, **kwargs):
        self._transform = None

    def transform(self, fn=None, n_jobs=-2, maxobs=None,
                  chunksize=50000, chunkfn=None, resolve='worker'):
        """

        Args:
            fn (func): the function to apply to each chunk, receives the
               chunk DataFrame (resolve==worker) or MDataFrame (resolve==function)
            n_jobs (int): number of jobs, defaults to CPU count - 1
            maxobs (int): number of max observations to process, defaults to
               len of mdf
            chunksize (int): max size of each chunk, defaults to 50000
            chunkfn (func): the function to chunk by
            outname (name): output collection name, defaults to _tmp_ prefix of
              input name
            resolve (string): worker, function. If worker is specified chunkfn
                receives the resolved DataFrame, otherwise receives the MDataFrame.
                Specify function to apply a custom resolving strategy (e.g. processing
                records one by one). Defaults to worker which uses .value on each
                chunk to resolve each chunk to a DataFrame before sending

        See Also:
            https://joblib.readthedocs.io/en/latest/generated/joblib.Parallel.html

        Returns:

        """
        self._transform = MTransform(self, fn=fn, n_jobs=n_jobs, maxobs=maxobs,
                                     chunksize=chunksize, chunkfn=chunkfn, resolve=resolve)
        return self

    def persist(self, name=None, store=None, append=False, local=False):
        """
        Persist the result of a .transform() in chunks

        Args:
            name (str): the name of the target dataset
            store (OmegaStore): the target store, defaults to om.datasets
            append (bool): if True will append the data, otherwise replace. Defaults
               to False
            local (bool): if True resolves a pending aggregation result into memory first, then persists
               result. Defaults to False, effectively persisting the result by the database without
               returning to the local process

        Returns:
            Metadata of persisted dataset
        """
        assert self._transform is not None
        outname, meta, outcoll = self._generate_outcoll(name, store, append)
        self._transform.run(outcoll, local=local)
        result = meta or self.__class__(outcoll, **self._getcopy_kwargs())
        return result

    def _generate_outcoll(self, name, store, append):
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
        return name, meta, coll


class MTransform:
    def __init__(self, in_lazy, fn=None, n_jobs=-2, maxobs=None,
                 chunksize=50000, chunkfn=None,
                 resolve='worker'):
        self.applyfn = fn or transform_nop
        self.n_jobs = n_jobs
        self.chunksize = chunksize
        self.maxobs = maxobs
        self.in_lazy = in_lazy
        self.resolve = resolve
        self.chunkfn = chunkfn or self._chunker

    def run(self, out_lazy=None, local=False, verbose=0):
        # setup mdf and parameters
        # prepare for serialization to remote worker
        out_lazy = out_lazy
        actual_njobs = self.n_jobs if not local else 1
        with Parallel(n_jobs=actual_njobs, backend='omegaml',
                      verbose=verbose) as p:
            # run in parallel
            # -- chunks is the generator that yields serializable chunks
            # -- runner is the delayed chunk processing function
            # -- jobs is the list of all chunks
            chunks = self.chunkfn(self.in_lazy, self.chunksize, self.maxobs)
            runner = delayed(transform_process_chunk)
            worker_resolves_mdf = self.resolve in ('worker', 'w')
            jobs = [runner(mdf, i, self.chunksize, self.applyfn, out_lazy, worker_resolves_mdf)
                    for i, mdf in enumerate(chunks)]
            p._backend._job_count = len(jobs)
            if verbose:
                print("Submitting {} tasks".format(len(jobs)))
            # finally submit
            p(jobs)
        return self.out_lazy

    def _chunker(self, in_lazy, chunksize, maxobs):
        for chunkdf in in_lazy.iterchunks(chunksize=chunksize):
            yield chunkdf


def transform_nop(ldf):
    # default transform that does no transformations
    pass


def transform_process_chunk(mdf, i, chunksize, applyfn, outcoll, worker_resolves):
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
        if worker_resolves:
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
