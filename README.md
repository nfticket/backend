Backend to aggregate tokens data.
=======

## Usage

`./nft_bot.py -h`

```
usage: nft_bot.py [-h] [-v VERBOSE] -e ELASTIC_HOST -k API_KEY -c CHAIN -a
                  ADDRESS [-t IDS] [-p {http,socks4,socks5}]


optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSE, --verbose VERBOSE
                        Logging level, for example: DEBUG or WARNING.
  -e ELASTIC_HOST, --elastic-host ELASTIC_HOST
                        Provides host:port for elasticsearch indexer.
  -k API_KEY, --api-key API_KEY
                        Provides API KEY for covalenthq.com service.
  -c CHAIN, --chain CHAIN
                        Provides Chain ID to parse.
  -a ADDRESS, --address ADDRESS
                        Provides contract address to parse.
  -t IDS, --ids IDS     Specified token IDs to collect. For example:
                        123,124,125
  -p {http,socks4,socks5}, --proxytype {http,socks4,socks5}
                        Set of proxy type.
```

## Example

Parse tokens for Binance Smart Chain (56 chain_id)  and Mina NFT:

`./court_bot.py -c 56 -a 0x828e90cef49b3baea41590ebf7f274d49f845dbb -e localhost:9200 -k api_key
