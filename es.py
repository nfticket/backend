import collections
import datetime
import gc
import sys
from elasticsearch import Elasticsearch, NotFoundError


ELASTICSEARCH_CONNECTION_PARAMS = {
    'hosts': '127.0.0.1:9200',
    'timeout': 60,
}

ELASTICSEARCH_DEFAULT_INDEX_SETTINGS = {
    "settings" : {
        "index" : {
            "number_of_replicas" : 1,
            'refresh_interval': '1s'
        },
        "analysis" : {
            "filter" : {
                'ru_stemming': {
                    'type': 'snowball',
                    'language': 'Russian',
                }
            },
            "analyzer" : {
                'russian': {
                    'type': 'custom',
                    'tokenizer': 'standard',
                    "filter": ['lowercase', 'stop', 'ru_stemming'],
                },
                "left" : {
                    "filter" : [
                        "standard",
                        "lowercase",
                        "stop"
                    ],
                    "type" : "custom",
                    "tokenizer" : "left_tokenizer"
                },
                "no_filter": {
                    "tokenizer" : "whitespace"
                }
            },
            "tokenizer" : {
                "left_tokenizer" : {
                    "side" : "front",
                    "max_gram" : 12,
                    "type" : "edgeNGram"
                }
            }
        }
    }
}


_elasticsearch_indices = collections.defaultdict(lambda: [])


def get_indices(indices=[]):
    if not _elasticsearch_indices:

        for index in indices:
            _elasticsearch_indices[index].append(None)

    if not indices:
        return _elasticsearch_indices
    else:
        result = {}
        for k, v in _elasticsearch_indices.items():
            if k in indices:
                result[k] = v
        return result


def create_aliases(es=None, indices=[]):
    es = es or Elasticsearch(**ELASTICSEARCH_CONNECTION_PARAMS)

    current_aliases = es.indices.get_alias()
    aliases_for_removal = collections.defaultdict(lambda: [])
    for item, tmp in current_aliases.items():
        for iname in list(tmp.get('aliases', {}).keys()):
            aliases_for_removal[iname].append(item)

    actions = []
    for index_alias, index_name in indices:
        for item in aliases_for_removal[index_alias]:
            actions.append({
                'remove': {
                    'index': item,
                    'alias': index_alias
                }
            })
        actions.append({
            'add': {
                'index': index_name,
                'alias': index_alias
            }
        })

    es.indices.update_aliases({'actions': actions})


def create_indices(es=None, indices=[], set_aliases=True):
    es = es or Elasticsearch(**ELASTICSEARCH_CONNECTION_PARAMS)

    result = []
    aliases = []

    now = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    for index_alias, _ in get_indices(indices).items():
        index_settings = ELASTICSEARCH_DEFAULT_INDEX_SETTINGS

        index_name = "{0}-{1}".format(index_alias, now)

        aliases.append((index_alias, index_name))

        type_mappings = {}
        # if we got any type mappings, put them in the index settings
        if type_mappings:
            index_settings['mappings'] = type_mappings

        es.indices.create(index_name, index_settings)

    if set_aliases:
        create_aliases(es, aliases)

    return result, aliases


def rebuild_indices(es=None, indices=[], set_aliases=True):
    es = es or Elasticsearch(**ELASTICSEARCH_CONNECTION_PARAMS)

    created_indices, aliases = create_indices(es, indices, False)

    # kludge to avoid OOM due to Django's query logging
    # db_logger = logging.getLogger('django.db.backends')
    # oldlevel = db_logger.level
    # db_logger.setLevel(logging.ERROR)

    current_index_name = None
    current_index_settings = {}

    def change_index():
        if current_index_name:
            # restore the original (or their ES defaults) settings back into
            # the index to restore desired elasticsearch functionality
            settings = {
                'number_of_replicas': current_index_settings.get('index', {}).get('number_of_replicas', 1),
                'refresh_interval': current_index_settings.get('index', {}).get('refresh_interval', '1s'),
            }
            es.indices.put_settings({'index': settings}, current_index_name)
            es.indices.refresh(current_index_name)

    for type_class, index_alias, index_name in created_indices:
        if index_name != current_index_name:
            change_index()

            # save the current index's settings locally so that we can restore them after
            current_index_settings = es.indices.get_settings(index_name).get(index_name, {}).get('settings', {})
            current_index_name = index_name

            # modify index settings to speed up bulk indexing and then restore them after
            es.indices.put_settings({'index': {
                'number_of_replicas': 0,
                'refresh_interval': '-1',
            }}, index=index_name)

        try:
            type_class.bulk_index(es, index_name)
        except:
            sys.stderr.write('`bulk_index` not implemented on `{}`.\n'.format(type_class._doc_type.index))
            continue
    else:
        change_index()

    # return to the norm for db query logging
    # db_logger.setLevel(oldlevel)

    if set_aliases:
        create_aliases(es, aliases)
        ELASTICSEARCH_DELETE_OLD_INDEXES = getattr(settings, 'ELASTICSEARCH_DELETE_OLD_INDEXES', True)
        if ELASTICSEARCH_DELETE_OLD_INDEXES:
            delete_indices(es, [a for a, i in aliases])

    return created_indices, aliases


def delete_indices(es=None, indices=[], only_unaliased=True):
    es = es or Elasticsearch(**ELASTICSEARCH_CONNECTION_PARAMS)
    indices = indices or get_indices(indices=[]).keys()
    indices_to_remove = []
    for index, aliases in es.indices.get_alias().items():
        # Make sure it isn't currently aliased, which would mean it's active (UNLESS
        # we want to force-delete all `simple_elasticsearch`-managed indices).
        #   AND
        # Make sure it isn't an arbitrary non-`simple_elasticsearch` managed index.
        if (not only_unaliased or not aliases.get('aliases')) and index.split('-', 1)[0] in indices:
            indices_to_remove.append(index)

    if indices_to_remove:
        es.indices.delete(','.join(indices_to_remove))
    return indices_to_remove
