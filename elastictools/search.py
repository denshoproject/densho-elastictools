from collections import OrderedDict
from copy import deepcopy
import json
import logging
logger = logging.getLogger(__name__)
import re
from urllib.parse import quote, urlunsplit

from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.query import QueryString

from . import docstore

DEFAULT_LIMIT = 1000

# whitelist of params recognized in URL query
#SEARCH_PARAM_WHITELIST = [
#    'fulltext',
#    'sort',
#    'topics',
#    'facility',
#    ...
#]

# fields where the relevant value is nested e.g. topics.id
#SEARCH_NESTED_FIELDS = [
#    'facility',
#    'topics',
#]

#SEARCH_AGG_FIELDS = {
#    'facility': 'facility.id',
#    'format': 'format',
#    'genre': 'genre',
#    'topics': 'topics.id',
#}

#SEARCH_MODELS = [
#    'ddrcollection',
#    'ddrentity',
#    'ddrsegment',
#]

# fields searched by query e.g. query will find search terms in these fields
#SEARCH_INCLUDE_FIELDS = [
#    'id',
#    'model',
#    'links_html',
#    'links_json',
#    'links_img',
#    'links_thumb',
#    'links_children',
#    'status',
#    'public',
#    'title',
#    'description',
#    ...
#]

#SEARCH_FORM_LABELS = {
#    'model': 'Model',
#    'facility': 'Facility',
#    'format': 'Format',
#    'geography.term': 'Geography',
#    ...
#}


def es_offset(pagesize, thispage):
    """Convert Django pagination to Elasticsearch limit/offset

    >>> es_offset(pagesize=10, thispage=1)
    0
    >>> es_offset(pagesize=10, thispage=2)
    10
    >>> es_offset(pagesize=10, thispage=3)
    20

    @param pagesize: int Number of items per page
    @param thispage: int The current page (1-indexed)
    @returns: int offset
    """
    page = thispage - 1
    if page < 0:
        page = 0
    return pagesize * page

def start_stop(limit, offset):
    """Convert Elasticsearch limit/offset to Python slicing start,stop

    >>> start_stop(10, 0)
    0,9
    >>> start_stop(10, 1)
    10,19
    >>> start_stop(10, 2)
    20,29
    """
    start = int(offset)
    stop = (start + int(limit))
    return start,stop

def limit_offset(request, results_per_page):
    """Get limit,offset values from request

    @param request
    @param results_per_page
    @returns: limit,offset
    """
    if request.GET.get('offset'):
        # limit and offset args take precedence over page
        limit = request.GET.get(
            'limit', int(request.GET.get('limit', results_per_page))
        )
        offset = request.GET.get('offset', int(request.GET.get('offset', 0)))
    elif request.GET.get('page'):
        limit = results_per_page
        thispage = int(request.GET['page'])
        offset = es_offset(limit, thispage)
    else:
        limit = results_per_page
        offset = 0
    return limit,offset

def django_page(limit, offset):
    """Convert Elasticsearch limit/offset pagination to Django page

    >>> django_page(limit=10, offset=0)
    1
    >>> django_page(limit=10, offset=10)
    2
    >>> django_page(limit=10, offset=20)
    3

    @param limit: int Number of items per page
    @param offset: int Start of current page
    @returns: int page
    """
    return divmod(offset, limit)[0] + 1

def es_host_name(conn):
    """Extracts host:port from Elasticsearch conn object.

    >>> es_host_name(Elasticsearch(settings.DOCSTORE_HOSTS))
    "<Elasticsearch([{'host': '192.168.56.1', 'port': '9200'}])>"

    @param conn: elasticsearch.Elasticsearch with hosts/port
    @returns: str e.g. "192.168.56.1:9200"
    """
    start = conn.__repr__().index('[') + 1
    end = conn.__repr__().index(']')
    text = conn.__repr__()[start:end].replace("'", '"')
    hostdata = json.loads(text)
    return ':'.join([hostdata['host'], str(hostdata['port'])])

def _strdammit(something):
    """Always return a str"""
    if isinstance(something, bool):
        if something:
            return 'True'
        else:
            return 'False'
    return str(something)


class SearchResults(object):
    """Nicely packaged search results for use in API and UI.

    >>> from rg import search
    >>> q = {"fulltext":"minidoka"}
    >>> sr = search.run_search(request_data=q, request=None)
    """

    def __init__(self, params={}, query={}, count=0, results=None, objects=[], limit=DEFAULT_LIMIT, offset=0):
        self.params = deepcopy(params)
        self.query = query
        self.aggregations = None
        self.objects = []
        self.total = 0
        try:
            self.limit = int(limit)
        except:
            self.limit = DEFAULT_LIMIT
        try:
            self.offset = int(offset)
        except:
            self.offset = 0
        self.start = 0
        self.stop = 0
        self.prev_offset = 0
        self.next_offset = 0
        self.prev_api = u''
        self.next_api = u''
        self.page_size = 0
        self.this_page = 0
        self.prev_page = 0
        self.next_page = 0
        self.prev_html = u''
        self.next_html = u''
        self.errors = []

        if results:
            # objects
            self.objects = [hit for hit in results]
            if results.hits.total:
                self.total = results.hits.total.value

            # aggregations
            self.aggregations = {}
            if hasattr(results, 'aggregations'):
                for field in results.aggregations.to_dict().keys():

                    # NOTE: Elasticsearch can only run fulltext queries on text
                    # fields.  Missing aggregations data here may be caused
                    # by non-text fields in SEARCH_INCLUDE_FIELDS.
                    # Elasticsearch will not throw an error. Aggregations will
                    # just not have the data that should be there.
                    # See https://github.com/denshoproject/ddr-public/issues/161

                    # nested aggregations
                    if field in ['topics', 'facility']:
                        field_ids = '{}_ids'.format(field)
                        aggs = results.aggregations[field]
                        self.aggregations[field] = aggs[field_ids].buckets

                    # simple aggregations
                    else:
                        aggs = results.aggregations[field]
                        self.aggregations[field] = aggs.buckets

                    #if VOCAB_TOPICS_IDS_TITLES.get(field):
                    #    self.aggregations[field] = []
                    #    for bucket in buckets:
                    #        if bucket['key'] and bucket['doc_count']:
                    #            self.aggregations[field].append({
                    #                'key': bucket['key'],
                    #                'label': VOCAB_TOPICS_IDS_TITLES[field].get(str(bucket['key'])),
                    #                'doc_count': str(bucket['doc_count']),
                    #            })
                    #            # print topics/facility errors in search results
                    #            # TODO hard-coded
                    #            if (field in ['topics', 'facility']) and not (isinstance(bucket['key'], int) or bucket['key'].isdigit()):
                    #                self.errors.append(bucket)
                    #
                    #else:
                    #    self.aggregations[field] = [
                    #        {
                    #            'key': bucket['key'],
                    #            'label': bucket['key'],
                    #            'doc_count': str(bucket['doc_count']),
                    #        }
                    #        for bucket in buckets
                    #        if bucket['key'] and bucket['doc_count']
                    #    ]

        elif objects:
            # objects
            self.objects = objects
            self.total = len(objects)

        else:
            self.total = count

        # elasticsearch
        self.prev_offset = self.offset - self.limit
        self.next_offset = self.offset + self.limit
        if self.prev_offset < 0:
            self.prev_offset = None
        if self.next_offset >= self.total:
            self.next_offset = None

        # django
        self.page_size = self.limit
        self.this_page = django_page(self.limit, self.offset)
        self.prev_page = u''
        self.next_page = u''
        # django pagination
        self.page_start = (self.this_page - 1) * self.page_size
        self.page_next = self.this_page * self.page_size
        self.pad_before = range(0, self.page_start)
        self.pad_after = range(self.page_next, self.total)

    def __repr__(self):
        try:
            q = self.params.dict()
        except:
            q = dict(self.params)
        if self.total:
            return u"<SearchResults [%s-%s/%s] %s>" % (
                self.offset, self.offset + self.limit, self.total, q
            )
        return u"<SearchResults [%s] %s>" % (self.total, q)

    def _make_prevnext_url(self, query, request):
        if request:
            # some rg_* browse values withspaces e.g. "short stories"
            # are not properly quoted
            path_info = request.path_info
            if ' ' in path_info:
                path_info = path_info.replace(' ', '%20')
            return urlunsplit([
                request.scheme,
                request.META.get('HTTP_HOST', 'testserver'),
                path_info,
                query,
                None,
            ])
        return '?%s' % quote(query)

    def to_dict(self, request, format_functions):
        """Express search results in API and Redis-friendly structure

        @param request: HttpRequest or RestRequest
        @param format_functions: dict
        returns: dict
        """
        if 'HttpRequest' in str(request.__class__):     # django HttpRequest
            params = request.GET.copy()
        elif 'rest_framework.request.' in str(request): # rest_framework Request
            params = request.query_params.dict()
        elif hasattr(self, 'params') and self.params:   # or just a dict
            params = deepcopy(self.params)
        return self._dict(params, {}, format_functions, request)

    def ordered_dict(self, request, format_functions, pad=False):
        """Express search results in API and Redis-friendly structure

        @param request: HttpRequest or RestRequest
        @param format_functions: dict
        returns: OrderedDict
        """
        if 'HttpRequest' in str(request.__class__):     # django HttpRequest
            params = request.GET.copy()
        elif 'rest_framework.request.' in str(request): # rest_framework Request
            params = request.query_params.dict()
        elif hasattr(self, 'params') and self.params:   # or just a dict
            params = deepcopy(self.params)
        return self._dict(params, OrderedDict(), format_functions, request, pad=pad)

    def _dict(self, params, data, format_functions, request, pad=False):
        """
        @param params: dict
        @param data: dict
        @param format_functions: dict
        @param pad: bool
        """
        data['total'] = self.total
        data['limit'] = self.limit
        data['offset'] = self.offset
        data['prev_offset'] = self.prev_offset
        data['next_offset'] = self.next_offset
        data['page_size'] = self.page_size
        data['this_page'] = self.this_page
        data['num_this_page'] = len(self.objects)
        if params.get('page'): params.pop('page')
        if params.get('limit'): params.pop('limit')
        if params.get('offset'): params.pop('offset')
        qs = [key + '=' + _strdammit(val) for key,val in params.items()]
        query_string = '&'.join(qs)
        data['prev_api'] = ''
        data['next_api'] = ''
        data['objects'] = []
        data['query'] = self.query
        data['aggregations'] = {}

        # pad before
        if pad:
            data['objects'] += [{'n':n} for n in range(0, self.page_start)]
        # page
        for o in self.objects:
            format_function = format_functions[o.meta.index]
            data['objects'].append(
                format_function(
                    document=o.to_dict(),
                    request=request,
                    listitem=True,
                )
            )
        # pad after
        if pad:
            data['objects'] += [{'n':n} for n in range(self.page_next, self.total)]

        # API prev/next
        if self.prev_offset != None:
            data['prev_api'] = self._make_prevnext_url(
                u'%s&limit=%s&offset=%s' % (
                    query_string, self.limit, self.prev_offset
                ),
                request
            )
        if self.next_offset != None:
            data['next_api'] = self._make_prevnext_url(
                u'%s&limit=%s&offset=%s' % (
                    query_string, self.limit, self.next_offset
                ),
                request
            )

        # Convert AttrDicts in aggregations to JSON-serializable list-of-dicts
        # (elasticsearch_dsl fails to do this).
        if self.aggregations:
            data['aggregations'] = {
                key: [attrdict.to_dict() for attrdict in agg]
                for key,agg in self.aggregations.items()
                if agg
            }

        return data


def sanitize_input(text):
    """Escape special characters

    http://lucene.apache.org/core/old_versioned_docs/versions/2_9_1/queryparsersyntax.html
    TODO Maybe elasticsearch-dsl or elasticsearch-py do this already
    """
    if isinstance(text, bool):
        return text
    assert (isinstance(text, str))

    BAD_SEARCH_CHARS = r'!+/:[\]^{}~'
    for c in BAD_SEARCH_CHARS:
        text = text.replace(c, '')
    text = text.replace('  ', ' ')

    # AND, OR, and NOT are used by lucene as logical operators.
    ## We need to escape these.
    # ...actually, we don't. We want these to be available.
    #for word in ['AND', 'OR', 'NOT']:
    #    escaped_word = "".join(["\\" + letter for letter in word])
    #    text = re.sub(
    #        r'\s*\b({})\b\s*'.format(word),
    #        r" {} ".format(escaped_word),
    #        text
    #    )

    # Escape odd quotes
    quote_count = text.count('"')
    if quote_count % 2 == 1:
        text = re.sub(r'(.*)"(.*)', r'\1\"\2', text)
    return text


class Searcher(object):
    """Wrapper around elasticsearch_dsl.Search

    >>> s = Searcher(index)
    >>> s.prep(request_data)
    'ok'
    >>> r = s.execute()
    'ok'
    >>> d = r.to_dict(request)
    """
    params = {}

    def __init__(self, ds, search=None):
        """
        @param ds: docstore.Docstore
        @param index: str Elasticsearch index name
        """
        self.ds = ds
        self.conn = self.ds.es
        self.s = search
        fields = []
        params = {}
        q = OrderedDict()
        query = {}
        sort_cleaned = None

    def __repr__(self):
        return u"<Searcher '%s', %s>" % (
            es_host_name(self.conn), self.params
        )

    def prepare(
        self,
        params,            # {}
        params_whitelist,  # SEARCH_PARAM_WHITELIST
        search_models,     # SEARCH_MODELS
        sort,              # []
        fields,            # SEARCH_INCLUDE_FIELDS
        fields_nested,     # SEARCH_NESTED_FIELDS
        fields_agg,        # SEARCH_AGG_FIELDS
    ):
        """Assemble elasticsearch_dsl.Search object

        @param params:           dict
        @param params_whitelist: list Accept only these (SEARCH_PARAM_WHITELIST)
        @param search_models:    list Limit to these ES doctypes (SEARCH_MODELS)
        @param sort:             list of legal Elasticsearch DSL sort arguments
        @param fields:           list Retrieve these fields (SEARCH_INCLUDE_FIELDS)
        @param fields_nested:    list See SEARCH_NESTED_FIELDS
        @param fields_agg:       dict See SEARCH_AGG_FIELDS
        @returns:
        """
        encycfront = False
        encycrg = False
        if 'published_encyc' in params.keys():
            encycfront = True
        elif 'published_rg' in params.keys():
            encycrg = True

        # gather inputs ------------------------------

        # self.params is a copy of the params arg as it was passed
        # to the method.  It is used for informational purposes
        # and is passed to SearchResults.
        # Sanitize while copying.
        if params:
            self.params = {
                key: sanitize_input(val)
                for key,val in params.items()
            }
        params = deepcopy(self.params)

        # scrub fields not in whitelist
        bad_fields = [
            key for key in params.keys()
            if key not in params_whitelist + ['page']
        ]
        for key in bad_fields:
            params.pop(key)

        indices = search_models
        if params.get('models'):
            indices = ','.join([self.ds.index_name(model) for model in models])

        s = Search(using=self.conn, index=indices)

        if encycrg:
            s = s.filter('term', published_rg=True)

        # only return specified fields
        s = s.source(fields)

        # sorting
        if sort:
            s = s.sort(*sort)

        if params.get('match_all'):
            s = s.query('match_all')
        elif params.get('fulltext'):
            fulltext = params.pop('fulltext')
            # MultiMatch chokes on lists
            if isinstance(fulltext, list) and (len(fulltext) == 1):
                fulltext = fulltext[0]
            # fulltext search
            s = s.query(
                QueryString(
                    query=fulltext,
                    fields=fields,
                    analyze_wildcard=False,
                    allow_leading_wildcard=False,
                    default_operator='AND',
                )
            )

        elif params.get('creators'):
            q = Q('bool',
                  must=[Q('nested',
                          path='creators',
                          query=Q('term', creators__namepart=params.pop('creators'))
                  )]
            )
            s = s.query(q)

        elif params.get('persons'):
            q = Q('bool',
                  must=[Q('nested',
                          path='persons',
                          query=Q('term', persons__namepart=params.pop('persons'))
                  )]
            )
            s = s.query(q)

        elif params.get('topics') or params.get('facility'):
            # SPECIAL CASE FOR DDRPUBLIC TOPICS, FACILITY BROWSE PAGES
            if params.get('topics'):
                q = Q('bool',
                      must=[Q('nested',
                              path='topics',
                              query=Q('term', topics__id=params.pop('topics'))
                      )]
                )
                s = s.query(q)
            elif params.get('facility'):
                q = Q('bool',
                      must=[Q('nested',
                              path='facility',
                              query=Q('term', facility__id=params.pop('facility'))
                      )]
                )
                s = s.query(q)

        if params.get('parent'):
            parent = params.pop('parent')
            if isinstance(parent, list) and (len(parent) == 1):
                parent = parent[0]
            if parent:
                parent = '%s-*' % parent
            s = s.query("wildcard", id=parent)

        # filters
        for key,val in params.items():

            if key in fields_nested:
                # Instead of nested search on topics.id or facility.id
                # search on denormalized topics_id or facility_id fields.
                fieldname = '%s_id' % key
                s = s.filter('term', **{fieldname: val})

                ## search for *ALL* the topics (AND)
                #for term_id in val:
                #    s = s.filter(
                #        Q('bool',
                #          must=[
                #              Q('nested',
                #                path=key,
                #                query=Q('term', **{'%s.id' % key: term_id})
                #              )
                #          ]
                #        )
                #    )

                ## search for *ANY* of the topics (OR)
                #s = s.query(
                #    Q('bool',
                #      must=[
                #          Q('nested',
                #            path=key,
                #            query=Q('terms', **{'%s.id' % key: val})
                #          )
                #      ]
                #    )
                #)

            elif (key in params_whitelist) and val:
                s = s.filter('term', **{key: val})
                # 'term' search is for single choice, not multiple choice fields(?)

        # aggregations
        for fieldname,field in fields_agg.items():

            # nested aggregation (Elastic docs: https://goo.gl/xM8fPr)
            if fieldname == 'topics':
                s.aggs.bucket('topics', 'nested', path='topics') \
                      .bucket('topics_ids', 'terms', field='topics.id', size=1000)
            elif fieldname == 'facility':
                s.aggs.bucket('facility', 'nested', path='facility') \
                      .bucket('facility_ids', 'terms', field='facility.id', size=1000)
                # result:
                # results.aggregations['topics']['topic_ids']['buckets']
                #   {u'key': u'69', u'doc_count': 9}
                #   {u'key': u'68', u'doc_count': 2}
                #   {u'key': u'62', u'doc_count': 1}

            # simple aggregations
            else:
                s.aggs.bucket(fieldname, 'terms', field=field)

        self.s = s

    def execute(self, limit, offset):
        """Execute a query and return SearchResults

        @param limit: int
        @param offset: int
        @returns: SearchResults
        """
        if not self.s:
            raise Exception('Searcher has no ES Search object.')
        start,stop = start_stop(limit, offset)
        response = self.s[start:stop].execute()
        for n,hit in enumerate(response.hits):
            hit.index = '%s %s/%s' % (n, int(offset)+n, response.hits.total)
        return SearchResults(
            params=self.params,
            query=self.s.to_dict(),
            results=response,
            limit=limit,
            offset=offset,
        )


def search(hosts, models=[], parent=None, filters=[], fulltext='', limit=10000, offset=0, page=None, aggregations=False):
    """Fulltext search using Elasticsearch query_string syntax.

    Note: More approachable, higher-level function than DDR.docstore.search.

    Full-text search strings:
        fulltext="seattle"
        fulltext="fusa OR teruo"
        fulltext="fusa AND teruo"
        fulltext="+fusa -teruo"
        fulltext="title:seattle"

    Note: Quoting inside strings is not (yet?) supported in the
    command-line version.

    Specify parent object and doctype/model:
        parent="ddr-densho-12"
        parent="ddr-densho-1000-1-"
        doctypes=['entity','segment']

    Filter on certain fields (filters may repeat):
        filter=['topics:373,27']
        filter=['topics:373', 'facility=12']

    @param hosts dict: settings.DOCSTORE_HOST
    @param models list: Restrict to one or more models.
    @param parent str: ID of parent object (partial OK).
    @param filters list: Filter on certain fields (FIELD:VALUE,VALUE,...).
    @param fulltext str: Fulltext search query.
    @param limit int: Results page size.
    @param offset int: Number of initial results to skip (use with limit).
    @param page int: Which page of results to show.
    """
    if not models:
        models = SEARCH_MODELS

    if filters:
        data = {}
        for f in filters:
            field,v = f.split(':')
            values = v.split(',')
            data[field] = values
        filters = data
    else:
        filters = {}

    if page and offset:
        Exception("Error: Specify either offset OR page, not both.")
    if page:
        thispage = int(page)
        offset = es_offset(limit, thispage)

    searcher = Searcher()
    searcher.prepare(params={
        'fulltext': fulltext,
        'models': models,
        'parent': parent,
        'filters': filters,
    })
    results = searcher.execute(limit, offset)
    return results
