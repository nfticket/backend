"""Parse NFT blockchains.
"""

import math
import logging
import argparse

from grab import Grab, proxylist
from grab.spider import Spider, Task

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch_dsl import Search


_CACHE_DB_NAME = 'nft'
_CACHE_DSN = 'postgres://zaebee@'

_API_URL = 'https://api.covalenthq.com/v1'
_OWNER_ENDPOINT = '{api}/{chain}/address/{owner}/balances_v2/?nft=true&page-number={page}'
_TOKENS_ENDPOINT = '{api}/{chain}/tokens/{address}/nft_token_ids/?page-number={page}'
_TOKENS_METADATA_ENDPOINT = '{api}/{chain}/tokens/{address}/nft_metadata/{token}/'
_TOKENS_TRANSACTIONS_ENDPOINT = '{api}/{chain}/tokens/{address}/nft_transactions/{token}/'

_PROXY_URL = (
    'https://www.proxyscan.io/api/proxy'
    '?last_check=6400'
    '&uptime=50'
    '&ping=500'
    '&limit=100'
    '&type={}'
    '&format=txt')

logging.basicConfig(
    datefmt='%:H%M:%S',
    filemode='a',
    filename='nft.log',
    level=logging.INFO)

PARSER = argparse.ArgumentParser()
PARSER.add_argument(
    '-v', '--verbose',
    type=str,
    default='INFO',
    help='Logging level, for example: DEBUG or WARNING.')
PARSER.add_argument(
    '-e', '--elastic-host',
    type=str,
    required=True,
    help='Provides host:port for elasticsearch indexer.')
PARSER.add_argument(
    '-k', '--api-key',
    type=str,
    required=True,
    help='Provides API KEY for covalenthq.com service.')
PARSER.add_argument(
    '-c', '--chain',
    type=int,
    required=True,
    help='Provides Chain ID to parse.')
PARSER.add_argument(
    '-a', '--address',
    type=str,
    required=True,
    help='Provides contract address to parse.')

PARSER.add_argument(
    '-t', '--ids',
    type=str,
    help='Specified token IDs to collect. For example: 123,124,125')

PARSER.add_argument(
    '-o', '--owner',
    type=str,
    help='Specified owner ID to collect NFTs.')

PARSER.add_argument(
    '-p', '--proxytype',
    type=str,
    default='http',
    choices=['http', 'socks4', 'socks5'],
    help='Set of proxy type.')


_MAPPING_META = {
    'AIRT': lambda r: r['nft'],
    'MNA_NFT': lambda r: r,
    'GEGO-V2': lambda r: r['result']['data'],
    'CocosNFT': lambda r: r['result']['data'],
}


def _get_pages(pagination):
    total = pagination['total_count']
    page_size = pagination['page_size']
    if not pagination['has_more']:
        return 0
    return math.ceil(total / float(page_size))


def _cast_attrs(nft_data):
    if not nft_data['external_data']:
        return nft_data
    if 'attributes' not in nft_data['external_data']:
        return nft_data
    attrs = nft_data['external_data']['attributes'] or []
    for i, _ in enumerate(attrs):
        attrs[i]['value'] = str(attrs[i]['value'])
    nft_data['external_data']['attributes'] = attrs
    return nft_data


class NFTSpider(Spider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._es = None
        self.token_counter = 0
        self.transaction_counter = 0
        self.cases_file = None

    @property
    def _api_key(self):
        return self.config.get('api_key', '')

    @property
    def _address(self):
        return self.config.get('address', '')

    @property
    def _owner(self):
        return self.config.get('owner', '')

    @property
    def _chain(self):
        return self.config.get('chain', 56)

    @property
    def _elastic_host(self):
        return self.config.get('elastic_host', 'localhost:9200')

    def prepare(self):
        self._es = Elasticsearch([self._elastic_host])

    def _build_url(self, endpoint, page=0, **kwargs):
        url = endpoint.format(
            api=_API_URL,
            chain=self._chain,
            address=self._address,
            api_key=self._api_key,
            owner=self._owner,
            page=page,
            **kwargs)
        return url

    def _bulk_index(self, docs, index=None, id_key='token_id', doc_type='nft'):
        """Save docs into Elasticsearch. Use contract address as index name."""
        body = []
        for doc in docs:
            if not doc[id_key]:
                continue
            data = {
                '_id': doc[id_key],
                '_type': doc_type,
                '_index': index or self._address}
            # bulk operation instructions/details
            data = {'update': data}
            body.append(data)
            body.append({'doc': doc, 'doc_as_upsert': True})
        if body:
            self._es.bulk(body, refresh=True)
        counter = self.token_counter
        if doc_type == 'transaction':
            counter = self.transaction_counter
        logging.info(
            'Indexed %s total: [%s]', doc_type.upper(), counter)

    def task_generator(self):
        page = 0
        if self.config.get('owner', None):
            url = self._build_url(_OWNER_ENDPOINT, page=page)
            yield Task('tokens', url, page=page, endpoint=_OWNER_ENDPOINT)
        ids = self.config.get('ids', '')
        query = Search(using=self._es).query(
            'match', token_id={'query': ids, 'operator': 'or'}
        ).source(['token_id'])
        for token in query.scan():
            t = token.to_dict()
            url = self._build_url(
                _TOKENS_METADATA_ENDPOINT, token=t['token_id'])
            yield Task('token_metadata', url, token=t)
        if not ids and not self._owner:
            url = self._build_url(_TOKENS_ENDPOINT)
            yield Task('tokens', url, page=page, endpoint=_TOKENS_ENDPOINT)

    def task_tokens(self, grab, task):
        tokens = grab.doc.json['data']['items']
        self.token_counter += len(tokens)
        self._bulk_index(tokens)
        for token in tokens:
            url = self._build_url(
                _TOKENS_METADATA_ENDPOINT, token=token['token_id'])
            if token['token_id']:
                yield Task('token_metadata', url, token=token)
        pages = _get_pages(grab.doc.json['data']['pagination'])
        for page in range(1, pages + 1):
            url = self._build_url(task.endpoint, page)
            yield Task('next_page', url, page=page, num_pages=pages)
        msg = 'Done NFT[address:%s][chain:%s][page:%s/%s]'
        logging.info(msg, self._address, self._chain, task.page, pages)

    def task_next_page(self, grab, task):
        msg = 'Done NFT[address:%s][chain:%s][page:%s/%s]'
        logging.info(
            msg, self._address, self._chain, task.page, task.num_pages)
        tokens = grab.doc.json['data']['items']
        self.token_counter += len(tokens)
        self._bulk_index(tokens)
        for token in tokens:
            url = self._build_url(
                _TOKENS_METADATA_ENDPOINT, token=token['token_id'])
            if token['token_id']:
                yield Task('token_metadata', url, token=token)

    def task_token_metadata(self, grab, task):
        token = task.token
        msg = 'Done metadata NFT[address:%s][token:%s]'
        logging.info(msg, self._address, token['token_id'])
        metadata = grab.doc.json['data']['items']
        if len(metadata) != 1:
            msg = 'NFT:%s has more than 1 metadata, please check: %s'
            logging.warning(msg, token['token_id'], task.url)
            return
        # TODO build array of token metadata for fastest updating.
        if 'nft_data' in metadata[0]:
            nft_data = metadata[0].pop('nft_data')
            token['nft_data'] = _cast_attrs(nft_data[0] if nft_data else {})
        token.update(metadata[0])
        url = token['nft_data'].get('token_url', None)
        if url:
            yield Task('token_external_data', url, token=token)
        else:
            self._bulk_index([token])
        url = self._build_url(
            _TOKENS_TRANSACTIONS_ENDPOINT, token=token['token_id'])
        yield Task('token_transactions', url, token=token)

    def task_token_external_data(self, grab, task):
        token = task.token
        msg = 'Done external NFT data: [address:%s][token:%s]'
        logging.info(msg, self._address, token['token_id'])
        if grab.doc.code == 200:
            data = grab.doc.json
            if token['contract_ticker_symbol'] in _MAPPING_META:
                metadata = _MAPPING_META[token['contract_ticker_symbol']](data)
                token['nft_data']['external_data'] = metadata
                token['nft_data'] = _cast_attrs(token['nft_data'])
        self._bulk_index([token])

    def task_token_transactions(self, grab, task):
        token = task.token
        msg = 'Done NFT transactions: [address:%s][token:%s]'
        logging.info(msg, self._address, token['token_id'])
        if grab.doc.code == 200:
            data = grab.doc.json['data']['items']
            if data:
                trx = data[0].get('nft_transactions', [])
                self.transaction_counter += len(trx)
                for tr in trx:
                    tr['token_id'] = token['token_id']
                index = 'transaction-%s' % self._address
                self._bulk_index(trx, index, 'tx_hash', 'transaction')


if __name__ == '__main__':
    arguments = PARSER.parse_args()
    proxytype = arguments.proxytype
    proxyurl = _PROXY_URL.format(
        'http,https' if proxytype == 'http' else proxytype)
    logging.root.setLevel(arguments.verbose)
    config = {
        'chain': arguments.chain,
        'address': arguments.address,
        'api_key': arguments.api_key,
        'elastic_host': arguments.elastic_host,
        'ids': arguments.ids,
        'owner': arguments.owner,
    }
    bot = NFTSpider(network_try_limit=5, thread_number=5, config=config)
    logging.info('Bot initialzed with config: %s', bot.config)
    bot.setup_cache('postgresql', _CACHE_DB_NAME, dsn=_CACHE_DSN)
    # bot.load_proxylist(proxyurl, 'url', proxytype)

    bot.run()

    logging.info(bot.render_stats())
    message = (
        'Total NFT[address:{address}][chain:{chain}]:\n'
        'Tokens: {tokens}\nTransactions: {transactions}').format(
            tokens=bot.token_counter, transactions=bot.transaction_counter,
            **config)
    logging.info(message)
