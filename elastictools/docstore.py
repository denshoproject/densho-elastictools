import json
import logging
logger = logging.getLogger(__name__)
from ssl import create_default_context
import sys

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout
from elasticsearch.exceptions import AuthenticationException, TransportError
from elasticsearch.exceptions import NotFoundError, RequestError, SerializationError
import elasticsearch_dsl

MAX_SIZE = 10000
DEFAULT_PAGE_SIZE = 20

SUCCESS_STATUSES = [200, 201]
STATUS_OK = ['completed']
PUBLIC_OK = [1,'1']


def get_elasticsearch(settings):
    """Gets Elasticsearch connection using app settings

    Will use an SSL certfile and/or HTTP Basic password if these are defined
    in config/settings.
    """
    # TODO simplify this once everything is using SSL/passwords
    if settings.DOCSTORE_SSL_CERTFILE and settings.DOCSTORE_PASSWORD:
        context = create_default_context(cafile=settings.DOCSTORE_SSL_CERTFILE)
        context.check_hostname = False
        return Elasticsearch(
            settings.DOCSTORE_HOST,
            scheme='https', ssl_context=context,
            port=9200,
            http_auth=(settings.DOCSTORE_USERNAME, settings.DOCSTORE_PASSWORD),
        )
    elif settings.DOCSTORE_SSL_CERTFILE:
        context = create_default_context(cafile=settings.DOCSTORE_SSL_CERTFILE)
        context.check_hostname = False
        return Elasticsearch(
            settings.DOCSTORE_HOST,
            scheme='https', ssl_context=context,
            port=9200,
        )
    else:
        return Elasticsearch(
            settings.DOCSTORE_HOST,
            scheme='http',
            port=9200,
        )


class Docstore():

    def __init__(self, index_prefix, host, settings, connection=None):
        self.index_prefix = index_prefix
        self.host = host
        if connection:
            self.es = connection
        else:
            self.es = get_elasticsearch(settings)

    def __repr__(self):
        return "<%s.%s %s:%s*>" % (
            self.__module__, self.__class__.__name__,
            self.host, self.index_prefix
        )

    def health(self):
        return self.es.cluster.health()

    def start_test(self):
        """Exit with an error if Elasticsearch cluster is unavailable

        IMPORTANT: This is meant to be run at application startup
        """
        try:
            self.es.cluster.health()
        except TransportError as err:
            logger.critical(f'Elasticsearch cluster unavailable')
            logger.critical(err)
            print(f'CRITICAL: Elasticsearch cluster unavailable')
            print(err)
        except AuthenticationException as err:
            logger.critical(f'Elasticsearch cluster auth error')
            logger.critical(err)
            print(f'CRITICAL: Elasticsearch cluster auth error')
            print(err)

    def status(self):
        """Returns status information from the Elasticsearch cluster.

        >>> docstore.Docstore().status()
        {
            u'indices': {
                u'ddrpublic-dev': {
                    u'total': {
                        u'store': {
                            u'size_in_bytes': 4438191,
                            u'throttle_time_in_millis': 0
                        },
                        u'docs': {
                            u'max_doc': 2664,
                            u'num_docs': 2504,
                            u'deleted_docs': 160
                        },
                        ...
                    },
                    ...
                }
            },
            ...
        }
        """
        return self.es.indices.stats()

    def index_name(self, model):
        """Returns indexname for specified model

        Indexes are named with an app prefix to prevent multiple apps from
        defining indexes with the same name.
        """
        return f'{self.index_prefix}{model}'

    def index_names(self):
        """Returns list of index names in use
        """
        return [name for name in list(self.status()['indices'].keys())]

    def index_exists(self, indexname):
        """Indicate whether the specified index exists
        """
        return self.es.indices.exists(index=indexname)

    def exists(self, model, document_id):
        """Indicate whether the specified document exists in the index

        @param model:
        @param document_id:
        """
        return self.es.exists(
            index=self.index_name(model),
            id=document_id
        )

    def url(self, model, document_id):
        """Return the Elasticsearch URL for the specified document

        @param model:
        @param document_id:
        """
        return f'http://{self.host}/{self.index_prefix}{model}/_doc/{document_id}'

    def get(self, model, es_class, document_id, fields=None):
        """Get the specified document

        @param model:
        @param es_class:
        @param document_id:
        @param fields: boolean Only return these fields
        @returns: repo_models.elastic.ESObject or None
        """
        return es_class.get(
            id=document_id,
            index=self.index_name(model),
            using=self.es,
            ignore=404,
        )

    def count(self, doctypes=[], query={}):
        """Executes a query and returns number of hits.

        The "query" arg must be a dict that conforms to the Elasticsearch query DSL.
        See docstore.search_query for more info.

        @param doctypes: list Type of object ('collection', 'entity', 'file')
        @param query: dict The search definition using Elasticsearch Query DSL
        @returns raw ElasticSearch query output
        """
        logger.debug('count(doctypes=%s, query=%s' % (doctypes, query))
        if not query:
            raise Exception(
                "Can't do an empty search. Give me something to work with here."
            )
        indices = ','.join(
            [f'{self.index_prefix}{m}' for m in doctypes]
        )
        doctypes = ','.join(doctypes)
        logger.debug(json.dumps(query))
        return self.es.count(
            index=indices,
            body=query,
        )

    def search(self, doctypes=[], query={}, sort=[], fields=[], from_=0, size=MAX_SIZE):
        """Executes a query, get a list of zero or more hits.

        The "query" arg must be a dict that conforms to the Elasticsearch query DSL.
        See docstore.search_query for more info.

        @param doctypes: list Type of object ('collection', 'entity', 'file')
        @param query: dict The search definition using Elasticsearch Query DSL
        @param sort: list of (fieldname,direction) tuples
        @param fields: str
        @param from_: int Index of document from which to start results
        @param size: int Number of results to return
        @returns raw ElasticSearch query output
        """
        logger.debug(
            'search(doctypes=%s, query=%s, sort=%s, fields=%s, from_=%s, size=%s' % (
                doctypes, query, sort, fields, from_, size
        ))
        if not query:
            raise Exception(
                "Can't do an empty search. Give me something to work with here."
            )

        indices = ','.join(
            [f'{self.index_prefix}{m}' for m in doctypes]
        )
        doctypes = ','.join(doctypes)
        logger.debug(json.dumps(query))
        clean_dict(sort)
        sort_cleaned = clean_sort(sort)
        fields = ','.join(fields)

        results = self.es.search(
            index=indices,
            body=query,
            sort=sort_cleaned,
            from_=from_,
            size=size,
            #_source_include=fields,  # TODO figure out fields
        )
        return results


def aggs_dict(aggregations):
    """Simplify aggregations data in search results

    input
    {
        u'format': {
            u'buckets': [{u'doc_count': 2, u'key': u'ds'}],
            u'doc_count_error_upper_bound': 0,
            u'sum_other_doc_count': 0
        },
        u'rights': {
            u'buckets': [{u'doc_count': 3, u'key': u'cc'}],
            u'doc_count_error_upper_bound': 0, u'sum_other_doc_count': 0
        },
    }
    output
    {
        u'format': {u'ds': 2},
        u'rights': {u'cc': 3},
    }
    """
    return {
        fieldname: {
            bucket['key']: bucket['doc_count']
            for bucket in data['buckets']
        }
        for fieldname,data in aggregations.items()
    }

def search_query(text='', must=[], should=[], mustnot=[], aggs={}):
    """Assembles a dict conforming to the Elasticsearch query DSL.

    Elasticsearch query dicts
    See https://www.elastic.co/guide/en/elasticsearch/guide/current/_most_important_queries.html
    - {"match": {"fieldname": "value"}}
    - {"multi_match": {
        "query": "full text search",
        "fields": ["fieldname1", "fieldname2"]
      }}
    - {"terms": {"fieldname": ["value1","value2"]}},
    - {"range": {"fieldname.subfield": {"gt":20, "lte":31}}},
    - {"exists": {"fieldname": "title"}}
    - {"missing": {"fieldname": "title"}}

    Elasticsearch aggregations
    See https://www.elastic.co/guide/en/elasticsearch/guide/current/aggregations.html
    aggs = {
        'formats': {'terms': {'field': 'format'}},
        'topics': {'terms': {'field': 'topics'}},
    }

    >>> from ui import docstore,format_json
    >>> t = 'posthuman'
    >>> a = [
        {'terms':{'language':['eng','chi']}},
        {'terms':{'creators.role':['distraction']}}
    ]
    >>> q = docstore.search_query(text=t, must=a)
    >>> print(format_json(q))
    >>> d = ['entity','segment']
    >>> f = ['id','title']
    >>> results = docstore.Docstore().search(doctypes=d, query=q, fields=f)
    >>> for x in results['hits']['hits']:
    ...     print x['_source']

    @param text: str Free-text search.
    @param must: list of Elasticsearch query dicts (see above)
    @param should:  list of Elasticsearch query dicts (see above)
    @param mustnot: list of Elasticsearch query dicts (see above)
    @param aggs: dict Elasticsearch aggregations subquery (see above)
    @returns: dict
    """
    body = {
        "query": {
            "bool": {
                "must": must,
                "should": should,
                "must_not": mustnot,
            }
        }
    }
    if text:
        body['query']['bool']['must'].append(
            {
                "match": {
                    "_all": text
                }
            }
        )
    if aggs:
        body['aggregations'] = aggs
    return body

def clean_dict(data):
    """Remove null or empty fields; ElasticSearch chokes on them.

    >>> d = {'a': 'abc', 'b': 'bcd', 'x':'' }
    >>> clean_dict(d)
    >>> d
    {'a': 'abc', 'b': 'bcd'}

    @param data: Standard DDR list-of-dicts data structure.
    """
    if data and isinstance(data, dict):
        for key in list(data.keys()):
            if not data[key]:
                del(data[key])

def clean_sort( sort ):
    """Take list of [a,b] lists, return comma-separated list of a:b pairs

    >>> clean_sort( 'whatever' )
    >>> clean_sort( [['a', 'asc'], ['b', 'asc'], 'whatever'] )
    >>> clean_sort( [['a', 'asc'], ['b', 'asc']] )
    'a:asc,b:asc'
    """
    cleaned = ''
    if sort and isinstance(sort,list):
        all_lists = [1 if isinstance(x, list) else 0 for x in sort]
        if not 0 in all_lists:
            cleaned = ','.join([':'.join(x) for x in sort])
    return cleaned

def cluster(clusters, ipaddr_port):
    """Indicate which cluster the docstore_host setting belongs to

    Sample config:
        docstore_clusters={"green":["192.168.0.19"],"blue":["192.168.0.20"], ...}
    """
    if isinstance(clusters, str):
        if clusters == '':
            return 'docstore_clusters is empty'
        try:
            clusters = json.loads(clusters)
        except json.decoder.JSONDecodeError:
            return 'JSONDecodeError on docstore_clusters'
    assert isinstance(clusters, dict)
    assert isinstance(ipaddr_port, str)
    _clusters_by_ip = {}
    for cluster,ips in clusters.items():
        for ip in ips:
            _clusters_by_ip[ip] = cluster
    ipaddr = ipaddr_port.split(':')[0]
    return _clusters_by_ip.get(ipaddr, 'unknown')
