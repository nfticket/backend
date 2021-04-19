"""Parse NFT blockchains.
"""

import math
import logging
import argparse

from grab import Grab, proxylist
from grab.spider import Spider, Task

from elasticsearch import Elasticsearch, NotFoundError


_CACHE_DB_NAME = 'nft'
_CACHE_DSN = 'postgres://zaebee@'

_API_URL = 'https://api.covalenthq.com/v1'
_TOKENS_ENDPOINT = '{api}/{chain}/tokens/{address}/nft_token_ids/?page-number={page}'
_TOKENS_METADATA_ENDPOINT = '{api}/{chain}/tokens/{address}/nft_metadata/{token}/'

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
    '-p', '--proxytype',
    type=str,
    default='http',
    choices=['http', 'socks4', 'socks5'],
    help='Set of proxy type.')


def _get_pages(pagination):
    total = pagination['total_count']
    page_size = pagination['page_size']
    if not pagination['has_more']:
        return 0
    return math.ceil(total / float(page_size))


def _cast_attrs(nft_data):
    try:
        attrs = nft_data['external_data']['attributes']
        for i, _ in enumerate(attrs):
            attrs[i]['value'] = str(attrs[i]['value'])
        nft_data['external_data']['attributes'] = attrs
        return nft_data
    except KeyError:
        return nft_data


class NFTSpider(Spider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._es = None
        self.counter = 0
        self.cases_file = None

    @property
    def _api_key(self):
        return self.config.get('api_key', '')

    @property
    def _address(self):
        return self.config.get('address', '')

    @property
    def _chain(self):
        return self.config.get('chain', 56)

    @property
    def _elastic_host(self):
        return self.config.get('elastic_host', 'localhost:9200')

    def prepare(self):
        self._es = Elasticsearch([self._elastic_host])

    def _build_url(self, endpoint, page=0, token=None):
        token = token or ''
        url = endpoint.format(
            api=_API_URL,
            chain=self._chain,
            address=self._address,
            api_key=self._api_key,
            token=token,
            page=page)
        return url

    def task_generator(self):
        url = self._build_url(_TOKENS_ENDPOINT)
        yield Task('first_page', url, page=0)

    def task_first_page(self, grab, task):
        tokens = grab.doc.json['data']['items']
        self._bulk_index(tokens)
        for token in tokens:
            url = self._build_url(
                _TOKENS_METADATA_ENDPOINT, token=token['token_id'])
            if token['token_id']:
                yield Task('token_metadata', url, token=token)
        pages = _get_pages(grab.doc.json['data']['pagination'])
        for page in range(1, pages + 1):
            url = self._build_url(_TOKENS_ENDPOINT, page)
            yield Task('next_page', url, page=page, num_pages=pages)
        msg = 'Done NFT[address:%s][chain:%s][page:%s/%s]'
        logging.info(msg, self._address, self._chain, task.page, pages)

    def task_next_page(self, grab, task):
        msg = 'Done NFT[address:%s][chain:%s][page:%s/%s]'
        logging.info(
            msg, self._address, self._chain, task.page, task.num_pages)
        tokens = grab.doc.json['data']['items']
        self._bulk_index(tokens)
        for token in tokens:
            url = self._build_url(
                _TOKENS_METADATA_ENDPOINT, token=token['token_id'])
            if token['token_id']:
                yield Task('token_metadata', url, token=token)

    def _bulk_index(self, tokens):
        body = []
        for token in tokens:
            if not token['token_id']:
                continue
            data = {
                '_index': self._address,
                '_id': token['token_id'],
                '_type': 'nft',
            }
            # bulk operation instructions/details
            data = {'update': data}
            body.append(data)
            body.append({'doc': token, 'doc_as_upsert': True})
        if body:
            self._es.bulk(body, refresh=True)
        self.counter += len(tokens)
        logging.info('Indexed NFT: [%s]', self.counter)

    def task_token_metadata(self, grab, task):
        token = task.token
        msg = 'Done NFT data: [address:%s][token:%s]'
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
        self._bulk_index([token])

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
        'elastic_host': arguments.elastic_host
    }
    bot = NFTSpider(network_try_limit=5, thread_number=5, config=config)
    logging.info('Bot initialzed with config: %s', bot.config)
    bot.setup_cache('postgresql', _CACHE_DB_NAME, dsn=_CACHE_DSN)
    # bot.load_proxylist(proxyurl, 'url', proxytype)

    bot.run()

    logging.info(bot.render_stats())
    message = (
        'Total NFT[address:{address}][chain:{chain}]: '
        '{total}').format(total=bot.counter, **config)
    logging.info(message)
