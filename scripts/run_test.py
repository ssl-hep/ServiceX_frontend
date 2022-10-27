# Tests against a ServiceX instance. This is not meant to be a complete
# test of every code path. Just a sanity check. The unit tests are meant to
# do that sort of testing.
# An example endpoint (pass as arg to this script):
#       http://localhost:5000
import argparse
import asyncio

from servicex import ServiceXDataset


async def run_query(endpoint: str, dest: str) -> None:
    ds = ServiceXDataset(
        "mc16_13TeV:mc16_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.deriv.DAOD_STDM3.e3601_e5984_s3126_r10201_r10210_p3975_tid20425969_00",  # NOQA
        backend_name=endpoint,
        max_workers=100)

    request = "(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')"  # NOQA
    if dest == 'rootfiles':
        r = ds.get_data_rootfiles(request)
        print(r)
    elif dest == 'rootfiles-minio':
        r = ds.get_data_rootfiles_async(request)
        async for f in r:  # type: ignore
            print(f)


if __name__ == '__main__':
    # Setup the arguments we can deal with here.
    parser = argparse.ArgumentParser(description='test servicex frontend.')
    parser.add_argument('--log', dest='logging', action='store_const',
                        const=True, default=False,
                        help='Name of endpoint to use from servicex.yaml file')
    parser.add_argument('--endpoint', dest='endpoint',
                        default='atlas')
    parser.add_argument('--output', dest='output',
                        default=['rootfiles'], nargs=1,
                        choices=['rootfiles', 'rootfiles-minio'],
                        help='What output format should be returned')
    args = parser.parse_args()

    if args.logging:
        import logging
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logging.getLogger('servicex').setLevel(logging.DEBUG)
        logging.getLogger('servicex').addHandler(ch)

    # servicex_adaptor = ServiceXAdaptor(sys.argv[1]) if len(sys.argv) >= 2 else None
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_query(args.endpoint, args.output[0]))
    finally:
        loop.close()
