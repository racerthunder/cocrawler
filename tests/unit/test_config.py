from unittest import mock

import cocrawler.config as config


def test_merge_dicts():
    a = {'a': {'a': 1}}
    b = {'b': {'b': 2}}

    c = config.merge_dicts(a, b)

    assert c == {'a': {'a': 1}, 'b': {'b': 2}}

    a = {'a': {'a': 1}, 'b': {'c': 3}}
    c = config.merge_dicts(a, b)

    assert c == {'a': {'a': 1}, 'b': {'b': 2, 'c': 3}}


def test_make_list():
    configfile = 'foo'
    ret = ['foo',
           '/home/kilroy/bar/.cocrawler-config.yml',
           '/home/kilroy/.cocrawler-config.yml',
           '/home/.cocrawler-config.yml']

    with mock.patch('cocrawler.config.os.getcwd', return_value='/home/kilroy/bar'):
        assert config.make_list(configfile) == ret


def test_type_fixup():
    tests = (('a', 'a'),
             ('a,b,c', 'a,b,c'),
             ('[a,b,c]', ['a', 'b', 'c']))

    for arg, result in tests:
        assert config.type_fixup(arg) == result


def main():
    import argparse
    import cocrawler.config as config
    import collections.abc

    ARGS = argparse.ArgumentParser(description='Cruzer web crawler')

    ARGS.add_argument('--loglevel', action='store', default='DEBUG')
    ARGS.add_argument('--reuse_session',action='store_true')
    ARGS.add_argument('--config', action='append')
    ARGS.add_argument('--configfile', action='store')
    ARGS.add_argument('--no-confighome', action='store_true')
    ARGS.add_argument('--no-test', action='store_true')
    ARGS.add_argument('--printdefault', action='store_true')
    ARGS.add_argument('--load', action='store',help='load previous state of the parser')

    args = ARGS.parse_args()

    config.config(args.configfile, args.config)

    print(config.print_final())

    ns = config.read('Fetcher', 'DNSWarmupLog')
    d = config.read('Crawl', 'MaxDepth')
    redir = config.read('Crawl', 'AllowExternalRedir')

    print(type(bool(redir)))
    if not redir:
        print('not allowed')

if __name__ == '__main__':
    #  --config Fetcher.Nameservers:1.1.1.1 --config Crawl.MaxDepth:30 --loglevel INFO --reuse_session
    main()
