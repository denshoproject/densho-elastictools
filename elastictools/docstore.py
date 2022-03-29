import json
import logging
logger = logging.getLogger(__name__)
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
        return Elasticsearch(
            f'{settings.DOCSTORE_HOST}',
            http_auth=(settings.DOCSTORE_USERNAME, settings.DOCSTORE_PASSWORD),
            client_cert=settings.DOCSTORE_SSL_CERTFILE,
            use_ssl=True, verify_certs=False, ssl_show_warn=False,
        )
    elif settings.DOCSTORE_SSL_CERTFILE:
        return Elasticsearch(
            f'{settings.DOCSTORE_HOST}',
            client_cert=settings.DOCSTORE_SSL_CERTFILE,
            use_ssl=True, verify_certs=False, ssl_show_warn=False,
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


class DocstoreManager(Docstore):
    """Subclass of Docstore with additional functions for managing indices
    """

    def __init__(self, index_prefix, host, settings, connection=None):
        super(DocstoreManager,self).__init__(index_prefix, host, settings, connection)

    def create_indices(self, classes):
        """Create indices for each model defined in ELASTICSEARCH_CLASSES

        @param classes: list of dicts w indexname,dsl_class (see create_index)
        """
        statuses = []
        for i in classes:
            status = self.create_index(
                self.index_name(i['doctype']),
                i['class']
            )
            statuses.append(status)
        return statuses

    def create_index(self, indexname, dsl_class):
        """Creates the specified index if it does not already exist.

        Uses elasticsearch-dsl classes defined in ELASTICSEARCH_CLASSES

        @param indexname: str
        @param dsl_class: elasticsearch_dsl.Document class
        @returns: JSON dict with status codes and responses
        """
        logger.debug('creating index {}'.format(indexname))
        if self.index_exists(indexname):
            status = '{"status":400, "message":"Index exists"}'
            logger.debug('Index exists')
            #print('Index exists')
        else:
            index = elasticsearch_dsl.Index(indexname)
            #print('index {}'.format(index))
            index.aliases(default={})
            #print('registering')
            out = index.document(dsl_class).init(index=indexname, using=self.es)
            if out:
                status = out
            elif self.index_exists(indexname):
                status = {
                    "name": indexname,
                    "present": True,
                }
            #print(status)
            #print('creating index')
        return status

    def delete_indices(self, classes):
        """Deletes indices for each model defined in ELASTICSEARCH_CLASSES

        @param classes: list of dicts w indexname,dsl_class (see create_index)
        """
        statuses = []
        for i in classes:
            status = self.delete_index(
                self.index_name(i['doctype'])
            )
            statuses.append(status)
        return statuses

    def delete_index(self, indexname):
        """Delete the specified index.

        @returns: JSON dict with status code and response
        """
        logger.debug('deleting index: %s' % indexname)
        if self.index_exists(indexname):
            status = self.es.indices.delete(index=indexname)
        else:
            status = {
                "name": indexname,
                "status": 500,
                "message": "Index does not exist",
            }
        logger.debug(status)
        return status

    def get_mappings(self):
        """Get mappings from Elasticsearch

        @returns: str JSON
        """
        return self.es.indices.get_mapping()

    def post_json(self, indexname, document_id, json_text):
        """POST the specified JSON document as-is.

        @param indexname: str
        @param document_id: str
        @param json_text: str JSON-formatted string
        @returns: dict Status info.
        """
        logger.debug('post_json(%s, %s)' % (indexname, document_id))
        return self.es.index(
            index=self.index_name(indexname),
            id=document_id,
            body=json_text
        )

    def delete(self, document_id, recursive=False):
        pass

    def reindex(self, source, dest):
        """Copy documents from one index to another.

        @param source: str Name of source index.
        @param dest: str Name of destination index.
        @returns: number successful,list of paths that didn't work out
        """
        logger.debug('reindex(%s, %s)' % (source, dest))
        if self.index_exists(source):
            logger.info('Source index exists: %s' % source)
        else:
            return '{"status":500, "message":"Source index does not exist"}'
        if self.index_exists(dest):
            logger.info('Destination index exists: %s' % dest)
        else:
            return '{"status":500, "message":"Destination index does not exist"}'
        version = self.es.info()['version']['number']
        logger.debug('Elasticsearch version %s' % version)
        if version >= '2.3':
            logger.debug('new API')
            body = {
                "source": {"index": source},
                "dest": {"index": dest}
            }
            results = self.es.reindex(
                body=json.dumps(body),
                refresh=None,
                requests_per_second=0,
                timeout='1m',
                wait_for_active_shards=1,
                wait_for_completion=False,
            )
        else:
            logger.debug('pre-2.3 legacy API')
            from elasticsearch import helpers
            results = helpers.reindex(
                self.es, source, dest,
                #query=None,
                #target_client=None,
                #chunk_size=500,
                #scroll=5m,
                #scan_kwargs={},
                #bulk_kwargs={}
            )
        return results

    def backup(self, repository_path, snapshot, indices=[]):
        """Make a snapshot backup of one or more Elasticsearch indices.

        repository = 'dev20190827'
        snapshot = 'dev-20190828-1007'
        indices = ['ddrpublic-dev', 'encyc-dev']
        agent = 'gjost'
        memo = 'backup before upgrading'
        from DDR import docstore
        ds = docstore.Docstore()
        ds.backup(repository, snapshot, indices, agent, memo)

        @param repository: str
        @param snapshot: str
        @param indices: list
        @returns: dict {"repository":..., "snapshot":...}
        """
        repository = os.path.basename(repository_path)
        client = SnapshotClient(self.es.cluster.client)
        # Get existing repository or make new one
        try:
            repo = client.get_repository(repository=repository)
        except TransportError:
            repo = client.create_repository(
                repository=repository,
                body={
                    "type": "fs",
                    "settings": {
                        "location": repository_path
                    }
                }
            )
        # Get snapshot info or initiate new one
        try:
            snapshot = client.get(repository=repository, snapshot=snapshot)
        except TransportError:
            body = {
                "indices": indices,
                "metadata": {},
            }
            snapshot = client.create(
                repository=repository, snapshot=snapshot, body=body
            )
        return {
            "repository": repo,
            "snapshot": snapshot,
        }

    def restore_snapshot(self, repository_path, snapshot, indices=[]):
        """Restore a snapshot
        """
        repository = os.path.basename(repository_path)
        client = SnapshotClient(self.es.cluster.client)
        repo = client.get_repository(repository=repository)
        result = client.restore(
            repository=repository_path,
            snapshot=snapshot,
            body={'indices': indices},
        )
        return result


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
