cache = {}


class CachedModelLoader:
    """ Caches models """
    def preload(self, store):
        to_preload = [meta for meta in store.list(raw=True) if meta.kind_meta.get('preload')]
        for meta in to_preload:
            if meta.name in cache and meta.modified < cache[meta.name]['modified']:
                continue
            model = store.get(meta.name)
            cache[meta.name] = {
                'model': model,
                'modified': meta.modified
            }
        return cache


