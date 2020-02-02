# Tests against a ServiceX instance. This is not meant to be a complete
# test of every code path. Just a sanity check. The unit tests are meant to
# do that sort of testing.
# An example endpoint (pass as arg to this script): http://localhost:5000/servicex
import sys
import ServiceX_fe
import asyncio


# TODO: This should not be async, but until we implement that bit, we will leave it like that.
async def run_query(endpoint: str) -> None:
    r = await ServiceX_fe.get_data_async("(call ResultTTree (call Select (call SelectMany (call EventDataset (list 'localds:bogus')) (lambda (list e) (call (attr e 'Jets') 'AntiKt4EMTopoJets'))) (lambda (list j) (/ (call (attr j 'pt')) 1000.0))) (list 'JetPt') 'analysis' 'junk.root')", "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_00", servicex_endpoint=endpoint)
    print(r)


if __name__ == '__main__':
    assert len(sys.argv) == 2
    asyncio.run(run_query(sys.argv[1]))
