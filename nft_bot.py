"""Parse NFT blockchains.
"""

import math
import logging
import itertools
import argparse

from grab import Grab, proxylist
from grab.spider import Spider, Task

from elasticsearch import Elasticsearch


_API_KEY = 'ckey_95c202a743e24270bc7f1706c4c'

_CACHE_DB_NAME = 'nft'
_CACHE_DSN = 'postgres://zaebee@'

_API_URL = 'https://api.covalenthq.com/v1'
_TOKENS_ENDPOINT = '{api}/{chain}/tokens/{address}/nft_token_ids/?page-number={page}'
_TOKENS_METADATA_ENDPOINT = '{api}/{chain}/tokens/nft_metadata/{token}'

_BASE_CONFIG = {'chain': 56}

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

parser = argparse.ArgumentParser()
parser.add_argument(
    '-v', '--verbose',
    type=str,
    default='INFO',
    help='Logging level, for example: DEBUG or WARNING.')
parser.add_argument(
    '-e', '--elastic-host',
    type=str,
    required=True,
    help='Provides host:port for elasticsearch indexer.')
parser.add_argument(
    '-k', '--api-key',
    type=str,
    required=True,
    help='Provides API KEY for covalenthq.com service.')
parser.add_argument(
    '-c', '--chain',
    type=int,
    required=True,
    help='Provides Chain ID to parse.')
parser.add_argument(
    '-a', '--address',
    type=str,
    required=True,
    help='Provides contract address to parse.')

parser.add_argument(
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

    def shutdown(self):
        self._es.indices.refresh(index='nft')

    def _build_url(self, endpoint, page=0):
        url = endpoint.format(
            api=_API_URL,
            chain=self._chain,
            address=self._address,
            api_key=self._api_key,
            page=page)
        return url

    def task_generator(self):
        url = self._build_url(_TOKENS_ENDPOINT)
        yield Task('first_page', url, page=0)

    def task_first_page(self, grab, task):
        resp = grab.doc.json['data']
        pages = _get_pages(resp['pagination'])
        self._bulk_index(resp['items'])
        logging.info('Done NFT[address:{}][chain:{}][page:{}/{}]'.format(
            self._address, self._chain, task.page, pages))
        for page in range(1, pages + 1):
            url = self._build_url(_TOKENS_ENDPOINT, page)
            logging.info('Fetch URL: %s' % url)
            yield Task('next_page', url, page=page, num_pages=pages)

    def task_next_page(self, grab, task):
        msg = ('Done NFT[address:{}][chain:{}][page:{}/{}]').format(
            self._address, self._chain, task.page, task.num_pages)
        logging.info(msg)
        resp = grab.doc.json['data']
        self._bulk_index(resp['items'])

    def _bulk_index(self, tokens):
        tmp = []

        for token in tokens:
            doc = token
            data = {
                '_index': self._address,
                '_type': 'nft',
                '_id': token['token_id']
            }
            data = {'update': data}
            # bulk operation instructions/details
            tmp.append(data)
            # only append bulk operation data if it's not a delete operation
            tmp.append({'doc': doc, 'doc_as_upsert': True})
        if tmp:
            self._es.bulk(tmp, refresh=True)
        self.counter += len(tokens)
        logging.info('Indexed NFT: [%s]', len(tokens))


if __name__ == '__main__':
    arguments = parser.parse_args()
    proxytype = arguments.proxytype
    proxyurl = _PROXY_URL.format(
            'http,https' if proxytype == 'http' else proxytype)
    logging.root.setLevel(arguments.verbose)
    config = _BASE_CONFIG.copy()
    config['chain'] = arguments.chain
    config['address'] = arguments.address
    config['api_key'] = arguments.api_key
    config['elastic_host'] = arguments.elastic_host

    bot = NFTSpider(network_try_limit=5, thread_number=2, config=config)
    logging.info('Bot initialzed with config: %s', bot.config)
    bot.setup_cache('postgresql', _CACHE_DB_NAME, dsn=_CACHE_DSN)
    # bot.load_proxylist(proxyurl, 'url', proxytype)

    bot.run()

    logging.info(bot.render_stats())
    message = (
        'Total NFT[address:{address}][chain:{chain}]: '
        '{total}').format(total=bot.counter, **config)
    logging.info(message)
